import { Share } from "@fastify-modular/route"
import Fastify from "fastify"
import { FastifyModular, SHARE_MANAGER, TimeUtility } from "fastify-modular"
import { pito } from "pito"
import tap from "tap"
import { KafkaModule } from "../esm/index.js"

tap.test('share', async t => {
    const PORT = 14000
    const route = Share("/event/share")
        .payload(pito.Obj({
            pong: pito.Num()
        }))
        .build()
    let counterPing = 0
    let counterPong = 0
    const fastify = Fastify({
        pluginTimeout : 300000
    })
    const mod = FastifyModular('test')
        .import(KafkaModule).from()
        .route(route).option({}).implements(async ({ payload }) => {
            console.log('pay:',payload )
            t.same(payload, { pong: counterPong })
            counterPong += 1
        })
        .build()
    try {
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
        await fastify.listen(PORT, '::')
        for await (const _ of TimeUtility.ticker(1000, { limit: 10 })) {
            fastify[SHARE_MANAGER]['kafka'].manager!.publish(route, { headers: {}, params: {}, payload: { pong: counterPing } })
            counterPing++
        }
        await TimeUtility.sleep(1000)
    } catch (err) {
        console.log(err)
        t.fail(`${err}`)
    } finally {
        await fastify.close()
    }
})