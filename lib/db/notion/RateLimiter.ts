import fs from 'fs'
import path from 'path'

interface QueueItem<T> {
  requestFunc: () => Promise<T>
  resolve: (value: T) => void
  reject: (err: any) => void
  key: string
  retries: number
}

export class RateLimiter {
  private queue: QueueItem<any>[] = []
  private inflight = new Set<string>()
  private isProcessing = false
  private lastRequestTime = 0
  private requestCount = 0
  private windowStart = Date.now()
  // 429错误时的全局暂停，避免并发重试
  private globalPauseUntil = 0

  constructor(
    private maxRequestsPerMinute = 60, // 降低到60请求/分钟，避免触发429
    private lockFilePath?: string,
    private maxRetries = 5 // 最大重试次数
  ) { }

  private async acquireLock() {
    if (!this.lockFilePath) return
    // 如果锁文件存在且创建时间过久（比如 >5分钟），认为是陈旧锁，直接删除
    if (fs.existsSync(this.lockFilePath)) {
      const stats = fs.statSync(this.lockFilePath)
      const age = Date.now() - stats.ctimeMs
      if (age > 30 * 1000) { // 30秒
        try {
          fs.unlinkSync(this.lockFilePath)
          console.warn('[限流] 删除陈旧锁文件:', this.lockFilePath)
        } catch (err) {
          console.error('[限流] 删除陈旧锁失败:', err)
        }
      }
    }
    while (true) {
      try {
        fs.writeFileSync(this.lockFilePath, process.pid.toString(), { flag: 'wx' })
        return
      } catch (err: any) {
        if (err.code === 'EEXIST') await new Promise(res => setTimeout(res, 100))
        else throw err
      }
    }
  }

  private releaseLock() {
    if (!this.lockFilePath) return
    try { if (fs.existsSync(this.lockFilePath)) fs.unlinkSync(this.lockFilePath) }
    catch (err) { console.error('释放锁失败', err) }
  }

  public enqueue<T>(key: string, requestFunc: () => Promise<T>): Promise<T> {
    if (this.inflight.has(key)) {
      return new Promise((resolve, reject) => {
        const interval = setInterval(() => {
          if (!this.inflight.has(key)) {
            clearInterval(interval)
            this.enqueue(key, requestFunc).then(resolve).catch(reject)
          }
        }, 50)
      })
    }

    return new Promise((resolve, reject) => {
      this.queue.push({ requestFunc, resolve, reject, key, retries: 0 })
      if (!this.isProcessing) this.processQueue()
    })
  }

  private async processQueue() {
    if (this.queue.length === 0) { this.isProcessing = false; return }
    this.isProcessing = true

    try {
      await this.acquireLock()

      // 检查全局暂停（429错误后）
      const now = Date.now()
      if (now < this.globalPauseUntil) {
        const waitTime = this.globalPauseUntil - now
        console.log(`[限流] 429后暂停中，等待 ${waitTime}ms`)
        await new Promise(res => setTimeout(res, waitTime))
      }

      const elapsed = now - this.windowStart

      if (elapsed > 60_000) { this.requestCount = 0; this.windowStart = now }
      if (this.requestCount >= this.maxRequestsPerMinute) {
        const waitTime = 60_000 - elapsed + 100
        console.log(`[限流] 达到每分钟上限 ${this.maxRequestsPerMinute}，等待 ${waitTime}ms`)
        await new Promise(res => setTimeout(res, waitTime))
        this.requestCount = 0
        this.windowStart = Date.now()
      }

      // 增加最小请求间隔到800ms，降低请求频率
      const minInterval = 800
      const intervalWait = Math.max(0, minInterval - (now - this.lastRequestTime))
      if (intervalWait > 0) await new Promise(res => setTimeout(res, intervalWait))

      const item = this.queue.shift()!
      const { requestFunc, resolve, reject, key, retries } = item
      this.inflight.add(key)

      try {
        const result = await requestFunc()
        this.lastRequestTime = Date.now()
        this.requestCount++
        resolve(result)
      } catch (err: any) {
        // 处理429错误，指数退避重试
        if (err?.status === 429 || err?.message?.includes('429')) {
          const retryDelay = Math.min(1000 * Math.pow(2, retries), 60000) // 指数退避，最大60秒
          console.warn(`[限流] 收到429错误，第${retries + 1}次重试，等待${retryDelay}ms`)

          if (retries < this.maxRetries) {
            // 设置全局暂停，避免其他请求也触发429
            this.globalPauseUntil = Date.now() + retryDelay

            // 重新加入队列
            this.queue.unshift({ ...item, retries: retries + 1 })
            this.inflight.delete(key)
            return
          }
        }
        reject(err)
      }
      finally { this.inflight.delete(key) }

    } catch (err) {
      console.error('限流队列异常', err)
    } finally {
      this.releaseLock()
      setTimeout(() => this.processQueue(), 0)
    }
  }
}
