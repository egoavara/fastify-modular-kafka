import { HTTPBody, HTTPNoBody, Share } from "@fastify-modular/route"
import Fastify from "fastify"
import { FastifyModular, TimeUtility } from "fastify-modular"
import { pito } from "pito"
import tap from "tap"
import { KafkaModule } from "../esm/index.js"

// tap.test('share', async t => {
//     const PORT = 14000
//     const route = Share("/event/share")
//         .payload(pito.Obj({
//             pong: pito.Num()
//         }))
//         .build()
//     let counterPing = 0
//     let counterPong = 0
//     const fastify = Fastify({
//         pluginTimeout: 300000
//     })
//     const mod = FastifyModular('test')
//         .import(KafkaModule).from()
//         .route(route).option({}).implements(async ({ payload }) => {
//             console.log('pay:', payload)
//             t.same(payload, { pong: counterPong })
//             counterPong += 1
//         })
//         .build()
//     try {
//         await fastify.register(
//             mod.plugin(),
//             {
//                 kafka: {
//                     kafka: { brokers: ['localhost:9092'] },
//                     default: {},
//                     producer: {},
//                 }
//             }
//         )
//         await fastify.listen(PORT, '::')
//         for await (const _ of TimeUtility.ticker(1000, { limit: 10 })) {
//             const k = KafkaModule.exports.kafka.find(fastify)
//             k!.publish(route, { headers: {}, params: {}, payload: { pong: counterPing } })
//             counterPing++
//         }
//         await TimeUtility.sleep(1000)
//     } catch (err) {
//         console.log(err)
//         t.fail(`${err}`)
//     } finally {
//         await fastify.close()
//     }
// })

tap.test("tx", async t => {
    const PORT = 14001

    const route = Share("/event/tx").payload(pito.Obj({ pong: pito.Num() })).build()
    const trigger = HTTPBody("POST", "/trigger").build()
    const fastify = Fastify({
        pluginTimeout: 300000
    })
    let unlock: any
    const lock = new Promise(resolve => unlock = resolve)
    const mod = FastifyModular('test')
        .import(KafkaModule).from()
        .route(route).implements(async ({ payload }) => {
            t.same(payload, { pong: 42 })
            unlock()
        })
        .route(trigger).implements(async ({ }, { txKafka }) => {
            (await txKafka).publish(route, { params: {}, payload: { pong: 42 } })
        })
        .build()
    await fastify.register(
        mod.plugin(),
        {
            kafka: {
                kafka: { brokers: ['localhost:9092'] },
                default: {},
                producer: {},
            }
        }
    )
    await fastify.listen({ port: PORT })
    await fastify.inject({ url: "/trigger", method: "POST" })
    await lock
    await fastify.close()

})