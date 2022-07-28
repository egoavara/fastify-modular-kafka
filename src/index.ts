import { InferShare, intoRegexTopic, Share } from "@fastify-modular/route"
import { pito } from "pito"
import { DEFAULT_SHARE_GROUP, FastifyModular, ObjectError, ShareManager, SHARE_MANAGER } from "fastify-modular"
import type {
    Consumer as _Consumer, ConsumerConfig as _ConsumerConfig,
    ConsumerRunConfig as _ConsumerRunConfig,
    ConsumerSubscribeTopics as _ConsumerSubscribeTopics,
    KafkaConfig as _KafkaConfig, Producer as _Producer, ProducerConfig as _ProducerConfig, Transaction as _Transaction
} from "kafkajs"
import { Kafka as _Kafka } from "kafkajs"

const DEFAULT_GROUP_ID = "@fastify-modular/kafka"

export type Kafka = Pick<_Kafka, keyof _Kafka>
export type Producer = Pick<_Producer, keyof _Producer>
export type Consumer = Pick<_Consumer, keyof _Consumer>
export type Transaction = Pick<_Transaction, keyof _Transaction>
export type ConsumerConfig = Pick<_ConsumerConfig, keyof _ConsumerConfig>
export type ConsumerRunConfig = Pick<_ConsumerRunConfig, keyof _ConsumerRunConfig>
export type ConsumerSubscribeTopics = Pick<_ConsumerSubscribeTopics, keyof _ConsumerSubscribeTopics>
export type KafkaConfig = Pick<_KafkaConfig, keyof _KafkaConfig>
export type ProducerConfig = Pick<_ProducerConfig, keyof _ProducerConfig>

export type KafkaModuleGroupOption = {
    consumer?: Omit<ConsumerConfig, 'groupId'>,
    run?: Omit<ConsumerRunConfig, 'eachMessage' | 'eachBatch'>,
    subscribe?: Omit<ConsumerSubscribeTopics, 'topics'>
}

export type KafkaModuleOption = {
    kafka: KafkaConfig,
    producer: ProducerConfig
    default: KafkaModuleGroupOption
    groups?: Record<string, KafkaModuleGroupOption>
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
    .static('kafka:raw', 'auto', async ({ }, option): Promise<Kafka> => {
        return new _Kafka(option.kafka)
    })
    .static('kafka', 'auto', async ({ "kafka:raw": kafkaRaw }, option) => {
        return {
            producer: kafkaRaw.producer(option.producer) as Producer,
            groupOptions: { [DEFAULT_GROUP_ID]: option.default, ...(option.groups ?? {}) } as Record<typeof DEFAULT_GROUP_ID, KafkaModuleGroupOption> & Record<string, KafkaModuleGroupOption>,
            consumers: {} as Record<string, Consumer>,
            groupTopics: {} as Record<string, (string | RegExp)[]>,
            regexMapping: [] as { gregex: RegExp, path: string }[],
        }
    })
    .dynamic("transaction:raw",
        5000,
        async ({ "kafka:raw": kafkaRaw, kafka }) => {
            await kafka.producer.connect()
            return await kafka.producer.transaction() as Transaction
        },
        async ({ value, catched },) => {
            if (catched === undefined) {
                await value.commit()
            } else {
                await value.abort()
            }
        }
    )
    .dynamic("transaction", 5000, async ({ "transaction:raw": txRaw }) => {
        const raw = await txRaw
        return {
            raw,
            async publish<Route extends Share<any, any, any, any, any>>(r: Route, args: {
                params: pito.Type<InferShare<Route>['Params']>,
                payload: pito.Type<InferShare<Route>['Payload']>,
                headers?: Record<string, string | string[]>,
                key?: string
            }): Promise<void> {
                const realTopic = realTopicName(r, args.params)
                await raw.send({
                    topic: realTopic,
                    messages: [
                        { value: JSON.stringify(args.payload), key: args.key, headers: args.headers }
                    ]
                })
            }
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
                                    path: path,
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
                    await kafka.producer.disconnect()
                    await Promise.all(Object.values(kafka.consumers).map(v => v.disconnect()))
                }
                initialized = false
            }
        }
        if (fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP] === undefined) { fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP] = { route: [], } }
        if (fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP].manager === undefined) { fastify[SHARE_MANAGER][DEFAULT_SHARE_GROUP].manager = manager; defaultManager = true }
        if (fastify[SHARE_MANAGER]['kafka'] === undefined) { fastify[SHARE_MANAGER]['kafka'] = { route: [], } }
        if (fastify[SHARE_MANAGER]['kafka'].manager === undefined) { fastify[SHARE_MANAGER]['kafka'].manager = manager; kafkaManager = true }
        //
        let reload = Promise.resolve()
        fastify.addHook("onReady", async function () {
            reload = manager.reload()
        })
        fastify.addHook("onClose", async (fastify) => {
            await reload
            await manager.stop()
        })
    })
    .build()
