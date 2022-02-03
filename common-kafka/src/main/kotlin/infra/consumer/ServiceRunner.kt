package infra.consumer

import java.util.concurrent.Executors

class ServiceRunner<T>(create: () -> ConsumerService<T>) {
    private var provider: ServiceProvider<T>

    init {
        this.provider = ServiceProvider(create)
    }

    fun start(threadCount: Int) {
        val pool = Executors.newFixedThreadPool(threadCount)
        for (i in 0..threadCount) {
            pool.submit(provider)
        }
    }
}
