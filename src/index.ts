import { InferShare, intoRegexTopic, Share } from "@fastify-modular/route"
import { DEFAULT_SHARE_GROUP, FastifyModular, ObjectError, ShareManager, SHARE_MANAGER } from "fastify-modular"
import rdkafka, { ConsumerGlobalConfig, ConsumerTopicConfig, KafkaConsumer, MessageHeader, MessageValue, Producer, ProducerGlobalConfig, ProducerTopicConfig } from "node-rdkafka"
import { pito } from "pito"

const DEFAULT_GROUP_ID = encodeURIComponent("@fastify-modular/kafka")


export type KafkaModuleGroupOption = {
    global: Omit<ConsumerGlobalConfig, 'group.id'>,
    topic?: ConsumerTopicConfig,
}



export type KafkaModuleOption = {
    producer: {
        global: ProducerGlobalConfig,
        topic?: ProducerTopicConfig
    },
    consumer?: Record<string, KafkaModuleGroupOption>
    default: KafkaModuleGroupOption
}

export type PublishArgs<Route extends Share<any, any, any, any, any>> = {
    params: pito.Type<InferShare<Route>['Params']>,
    payload: pito.Type<InferShare<Route>['Payload']>,
    headers?: Record<string, string>,
    key?: string
}

export type Kafka = {
    producer: Producer,
    consumers: Record<string, KafkaConsumer>,
    groupOptions: Record<string, KafkaModuleGroupOption>,
    regexMapping: { gregex: RegExp, path: string }[],
    publish<Route extends Share<any, any, any, any, any>>(r: Route, ...args: PublishArgs<Route>[]): Promise<void>
}
interface KafkaMessage { value: MessageValue, key?: string, headers?: MessageHeader[] }

export type TxKafka = {
    stored: Record<string, KafkaMessage[]>
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
    // .static('kafka:raw', 'auto', async ({ }, option): Promise<KafkaJs> => {

    //     return new KafkaJs(option.kafka)
    // })
    .static('kafka', 'auto', async ({ }, option): Promise<Kafka> => {
        const producer = new rdkafka.Producer(option.producer.global, option.producer.topic)
        const temp: Kafka = {
            producer,
            consumers: {},
            groupOptions: {},
            regexMapping: [] as { gregex: RegExp, path: string }[],
            async publish(r, ...args) {
                if (!producer.isConnected()) {
                    await new Promise<void>(resolve => { producer.once("ready", () => resolve()) })
                }
                const aggre: Record<string, KafkaMessage[]> = {}
                for (const arg of args) {
                    const realTopic = realTopicName(r, arg.params)
                    if (!Object.hasOwn(aggre, realTopic)) {
                        aggre[realTopic] = []
                    }
                    const msg = Buffer.from(JSON.stringify(arg.payload), "utf-8")
                    aggre[realTopic].push({
                        value: msg,
                        headers: arg.headers === undefined ? undefined : [arg.headers],
                        key: arg.key,
                    })
                }
                await Promise.all(Object.entries(aggre).map(async ([topic, messages]) => {
                    for (const msg of messages) {
                        try {
                            producer.produce(topic, undefined, msg.value, msg.key, undefined, undefined, msg.headers)
                        } catch {
                            await new Promise<void>(resolve => { producer.flush(undefined, () => { resolve() }) })
                            producer.produce(topic, undefined, msg.value, msg.key, undefined, undefined, msg.headers)
                        }
                    }
                    await new Promise<void>(resolve => { producer.flush(undefined, () => { resolve() }) })
                }))
            }
        }
        temp.producer.connect()

        return temp
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
                        const msg = Buffer.from(JSON.stringify(arg.payload), "utf-8")
                        this.stored[realTopic].push({
                            value: msg,
                            headers: arg.headers === undefined ? undefined : [arg.headers],
                            key: arg.key,
                        })
                    }
                }
            }
            return result
        },
        async ({ value, catched }, { kafka }) => {
            if (catched === undefined) {
                await Promise.all(Object.entries(value.stored).map(async ([topic, messages]) => {
                    for (const msg of messages) {
                        try {
                            kafka.producer.produce(topic, undefined, msg.value, msg.key, undefined, undefined, msg.headers)
                        } catch {
                            await new Promise<void>(resolve => { kafka.producer.flush(undefined, () => { resolve() }) })
                            kafka.producer.produce(topic, undefined, msg.value, msg.key, undefined, undefined, msg.headers)
                        }
                    }
                    await new Promise<void>(resolve => { kafka.producer.flush(undefined, () => { resolve() }) })
                }))
            }
        })
    .do(async ({ kafka }, option, { fastify, instance }) => {
        let initialized = false
        let defaultManager = false
        let kafkaManager = false
        // 
        const manager: ShareManager & InternalShareManager = {
            instance,
            async publish(route, arg) {
                await kafka.publish(route, arg)
            },
            async reload() {
                // =====
                if (initialized) {
                    await this.stop()
                }
                initialized = true
                // =====
                kafka.consumers = {}
                kafka.groupOptions = {}
                kafka.regexMapping = []
                // 
                const groupTopics: Record<string, (string | RegExp)[]> = {}
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
                        kafka.consumers[gid] = new rdkafka.KafkaConsumer({ "group.id": gid, ...kafka.groupOptions[gid].global, }, kafka.groupOptions[gid].topic ?? {})
                    }
                    if (!(gid in groupTopics)) {
                        groupTopics[gid] = []
                    }
                    if (route.topic.search(':') === -1) {
                        groupTopics[gid].push(route.topic)
                        kafka.regexMapping.push({
                            gregex: intoRegexTopic(route.topic, { namedRegex: true }),
                            path: route.path.replace(":", "_")
                        })
                    } else {
                        groupTopics[gid].push(intoRegexTopic(route.topic, { namedRegex: false }))
                        kafka.regexMapping.push({
                            gregex: intoRegexTopic(route.topic, { namedRegex: true }),
                            path: route.path.replace(":", "_")
                        })
                    }
                }
                // =====

                await Promise.all(Object.entries(kafka.consumers).map(async ([gid, v]) => {
                    v.connect()
                    return new Promise<void>((resolve) => {
                        v.once("ready", () => {
                            v.subscribe(groupTopics[gid]);
                            v.getMetadata()
                            resolve()
                        })
                        v.on("data", (payload) => {
                            const message = payload.value?.toString()
                            for (const { gregex, path } of kafka.regexMapping) {
                                const matched = payload.topic.match(gregex)
                                if (matched === null) {
                                    continue
                                }
                                const params = matched.groups ?? {}

                                fastify.inject({
                                    method: "PATCH",
                                    url: path,
                                    headers: { 'content-type': 'application/json' },
                                    payload: JSON.stringify({
                                        topic: payload.topic,
                                        payload: message === undefined ? undefined : JSON.parse(message),
                                        params: params,
                                        headers: payload.headers === undefined ? {} : Object.assign({}, ...payload.headers),
                                        key: payload.key?.toString()
                                    })
                                }).then((response) => {
                                    if (response.statusCode !== 204) {
                                        throw new ObjectError({})
                                    }
                                }).catch((err) => {
                                    if (err instanceof Error) {
                                        fastify.log.error({ error: err.message, cause: err.cause, stack: err.stack }, "kafka failed")
                                        return
                                    }
                                    fastify.log.error({ error: JSON.stringify(err) }, "kafka failed")
                                })
                                break
                            }
                        })
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
