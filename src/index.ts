import { InferShare, intoRegexTopic, Share } from "@fastify-modular/route"
import { DEFAULT_SHARE_GROUP, FastifyModular, ObjectError, ShareManager, SHARE_MANAGER } from "fastify-modular"
import type { Consumer, ConsumerConfig, ConsumerRunConfig, ConsumerSubscribeTopics, KafkaConfig, Message, Producer, ProducerConfig, Transaction } from "kafkajs"
import { Kafka as KafkaJs } from "kafkajs"
import { pito } from "pito"

const DEFAULT_GROUP_ID = "@fastify-modular/kafka"


export type KafkaModuleGroupOption = {
    consumer?: Omit<ConsumerConfig, 'groupId'>
    run?: Omit<ConsumerRunConfig, 'eachMessage' | 'eachBatch'>
    subscribe?: Omit<ConsumerSubscribeTopics, 'topics'>
}

export type KafkaModuleOption = {
    kafka: KafkaConfig
    producer: ProducerConfig
    default: KafkaModuleGroupOption
    groups?: Record<string, KafkaModuleGroupOption>
}

export type PublishArgs<Route extends Share<any, any, any, any, any>> = {
    params: pito.Type<InferShare<Route>['Params']>,
    payload: pito.Type<InferShare<Route>['Payload']>,
    headers?: Record<string, string | string[]>,
    key?: string
}
export type Kafka = {
    producer: Producer,
    groupOptions: Record<typeof DEFAULT_GROUP_ID, KafkaModuleGroupOption> & Record<string, KafkaModuleGroupOption>,
    consumers: Record<string, Consumer>,
    groupTopics: Record<string, (string | RegExp)[]>,
    regexMapping: { gregex: RegExp, path: string }[],
    publish<Route extends Share<any, any, any, any, any>>(r: Route, ...args: PublishArgs<Route>[]): Promise<void>

}
export type TxKafka = {
    stored: Record<string, Message[]>
    publish<Route extends Share<any, any, any, any, any>>(r: Route, ...args: PublishArgs<Route>[]): Promise<void>;
}

type InternalShareManager = {
    stop(): Promise<void>
}

function realTopicName(share: Share<any, any, any, any, any>, params: unknown) {
    let topic = share.topic as string
    if (typeof params === 'object' && params !== null) {
        for (const [k, v] of Object.entries(params)) {
            if (typeof v === 'string') {
                topic = topic.replace(`:${k}`, v)
            } else if (typeof v === 'number') {
                topic = topic.replace(`:${k}`, String(v))
            } else if (typeof v === 'boolean') {
                topic = topic.replace(`:${k}`, String(v))
            } else if (typeof v === 'bigint') {
                topic = topic.replace(`:${k}`, String(v))
            } else {
                throw new Error(`unexpected params, { ..., ${k} : ${(params as Record<string, any>)[k]} => ${v}, ...`)
            }
        }
    }
    if (topic.indexOf(':') !== -1) {
        throw new Error(`unresolved topic name ${share.topic} => ${topic}`)
    }
    return topic
}
export const KafkaModule = FastifyModular('kafka')
    .option<KafkaModuleOption>()
    .static('kafka:raw', 'auto', async ({ }, option): Promise<KafkaJs> => {
        return new KafkaJs(option.kafka)
    })
    .static('kafka', 'auto', async ({ "kafka:raw": kafkaRaw }, option): Promise<Kafka> => {
        const producer = kafkaRaw.producer(option.producer)
        await producer.connect()
        return {
            producer,
            groupOptions: { [DEFAULT_GROUP_ID]: option.default, ...(option.groups ?? {}) } as Record<typeof DEFAULT_GROUP_ID, KafkaModuleGroupOption> & Record<string, KafkaModuleGroupOption>,
            consumers: {} as Record<string, Consumer>,
            groupTopics: {} as Record<string, (string | RegExp)[]>,
            regexMapping: [] as { gregex: RegExp, path: string }[],
            async publish(r, ...args) {
                const aggre: Record<string, Message[]> = {}
                for (const arg of args) {
                    const realTopic = realTopicName(r, arg.params)
                    if (!Object.hasOwn(aggre, realTopic)) {
                        aggre[realTopic] = []
                    }
                    aggre[realTopic].push({ value: JSON.stringify(arg.payload), key: arg.key, headers: arg.headers })
                }
                await Promise.all(Object.entries(aggre).map(async ([topic, messages]) => {
                    await producer.send({
                        topic,
                        messages
                    })
                }))
            }
        }
    })
    .dynamic("txKafka", 5000,
        async ({ }): Promise<TxKafka> => {
            const result: TxKafka = {
                stored: {},
                async publish(r, ...args) {
                    for (const arg of args) {
                        const realTopic = realTopicName(r, arg.params)
                        if (!Object.hasOwn(this.stored, realTopic)) {
                            this.stored[realTopic] = []
                        }
                        this.stored[realTopic].push({ value: JSON.stringify(arg.payload), key: arg.key, headers: arg.headers })
                    }
                }
            }
            return result
        },
        async ({ value, catched }, { kafka }) => {
            if (catched === undefined) {
                value.stored
                kafka.producer.sendBatch({
                    topicMessages: Object.entries(value.stored).map(([k, v]) => {
                        return {
                            topic: k,
                            messages: v
                        }
                    })
                })
            }
        })
    .do(async ({ kafka, "kafka:raw": kafkaRaw }, option, { fastify, instance }) => {
        let initialized = false
        let defaultManager = false
        let kafkaManager = false
        // 
        const manager: ShareManager & InternalShareManager = {
            instance,
            async publish(route, args) {
                const realTopic = realTopicName(route, args.params)
                await kafka.producer.send({
                    topic: realTopic,
                    messages: [
                        { value: JSON.stringify(args.payload), key: args.key, headers: args.headers }
                    ]
                })
            },
            async reload() {
                // =====
                if (initialized) {
                    await this.stop()
                }
                initialized = true
                // =====
                kafka.consumers = {}
                kafka.groupTopics = {}
                kafka.regexMapping = []
                for (const { define: route, option: routeOption } of [...(kafkaManager ? fastify[SHARE_MANAGER]['kafka'].route : []), ...(defaultManager ? fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP].route : [])]) {
                    const gid = defaultManager ? routeOption.groupId ?? DEFAULT_GROUP_ID : routeOption.groupId
                    if (gid === undefined) {
                        continue
                    }
                    if (!(gid in kafka.groupOptions)) {
                        kafka.groupOptions[gid] = option.default
                        fastify.log.warn(`fastify-modular(kafka) : not have option for group id = '${gid}', it use default kafka consumer option`)
                    }
                    if (!(gid in kafka.consumers)) {
                        kafka.consumers[gid] = kafkaRaw.consumer({
                            ...(kafka.groupOptions[gid].consumer ?? {}),
                            groupId: gid
                        })
                    }
                    if (!(gid in kafka.groupTopics)) {
                        kafka.groupTopics[gid] = []
                    }
                    if (route.topic.search(':') === -1) {
                        kafka.groupTopics[gid].push(route.topic)
                        kafka.regexMapping.push({
                            gregex: intoRegexTopic(route.topic, { namedRegex: true }),
                            path: route.path.replace(":", "_")
                        })
                    } else {
                        const regexNongroups = intoRegexTopic(route.topic, { namedRegex: false })
                        kafka.groupTopics[gid].push(regexNongroups)
                        kafka.regexMapping.push({
                            gregex: intoRegexTopic(route.topic, { namedRegex: true }),
                            path: route.path.replace(":", "_")
                        })
                    }
                }
                // =====
                await kafka.producer.connect()

                await Promise.all(Object.values(kafka.consumers).map(async v => {
                    await v.connect()
                }))

                await Promise.all(Object.entries(kafka.consumers).map(([gid, v]) => {
                    return v.subscribe({ topics: kafka.groupTopics[gid], ...(kafka.groupOptions[gid].subscribe) })
                }))

                await Promise.all(Object.entries(kafka.consumers).map(([gid, v]) => {
                    return v.run({
                        ...(kafka.groupOptions[gid].run),
                        async eachMessage(payload) {
                            const message = payload.message.value?.toString()
                            
                            for (const { gregex, path } of kafka.regexMapping) {
                                const matched = payload.topic.match(gregex)
                                if (matched === null) {
                                    continue
                                }
                                const params = matched.groups ?? {}
                                const response = await fastify.inject({
                                    method: "PATCH",
                                    url: path,
                                    headers: { 'content-type': 'application/json' },
                                    payload: JSON.stringify({
                                        topic: payload.topic,
                                        payload: message === undefined ? undefined : JSON.parse(message),
                                        params: params,
                                        headers: payload.message.headers ?? {},
                                        key: payload.message.key?.toString()
                                    })
                                })
                                if (response.statusCode !== 204) {
                                    throw new ObjectError({})
                                }
                                // 
                                break
                            }
                            // 
                        }
                    })
                }))
            },
            async stop() {
                if (initialized) {
                    initialized = false
                    await kafka.producer.disconnect()
                    await Promise.all(Object.values(kafka.consumers).map(v => v.disconnect()))
                }
            }
        }
        if (fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP] === undefined) { fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP] = { route: [], } }
        if (fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP].manager === undefined) { fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP].manager = manager; defaultManager = true }
        if (fastify[SHARE_MANAGER]['kafka'] === undefined) { fastify[SHARE_MANAGER]['kafka'] = { route: [], } }
        if (fastify[SHARE_MANAGER]['kafka'].manager === undefined) { fastify[SHARE_MANAGER]['kafka'].manager = manager; kafkaManager = true }
        //
        fastify.addHook("onReady", async function () {
            manager.reload()
        })
        fastify.addHook("onClose", async (fastify) => {
            await manager.stop()
        })
    })
    .build()
