package com.example.sensorlogger

import kotlin.math.max

/**
 * LA-triggered projection with gyro-predicted orientation.
 * Keeps its own small state: gyro/orientation buffers and v accumulator.
 */
class ProjectionEngineLA(
    private val predWindowSec: Double = 0.25,    // buffer horizon
    private val maxPredMs: Long = 50L,           // cap forward span
) {
    private val gyroBuf = ArrayDeque<GyroSample>()
    private val oriBuf  = ArrayDeque<OriSample>()
    private var vAccum  = 0.0

    fun clear() {
        gyroBuf.clear(); oriBuf.clear(); vAccum = 0.0
    }

    fun onGyro(s: GyroSample) {
        gyroBuf.addLast(s)
        trim(s.ts)
    }

    fun onOri(o: OriSample) {
        oriBuf.addLast(o)
        trim(o.ts)
    }

    private fun trim(nowTs: Long) {
        val horizonNs = (predWindowSec * 1e9).toLong()
        while (gyroBuf.isNotEmpty() && (nowTs - gyroBuf.first().ts) > horizonNs) gyroBuf.removeFirst()
        while (oriBuf.isNotEmpty()  && (nowTs - oriBuf.first().ts)  > horizonNs) oriBuf.removeFirst()
    }

    /**
     * Compute P_pred & V_pred for given LA sample at time laTsNs with device-frame LA (ax,ay,az)
     * dirFx, dirFy define the forward unit vector in ENU (East/North).
     */
    fun onLa(laTsNs: Long, ax: Float, ay: Float, az: Float, dirFx: Double, dirFy: Double): PredResult? {
        // 1) find latest orientation <= laTsNs
        var base: OriSample? = null
        val it = oriBuf.iterator()
        while (it.hasNext()) {
            val o = it.next()
            if (o.ts <= laTsNs) base = o else break
        }
        base ?: return null

        val spanNs = (laTsNs - base.ts).coerceAtLeast(0L)
        val maxSpanNs = maxPredMs * 1_000_000L
        if (spanNs > maxSpanNs) return null

        // 2) integrate gyro between base.ts..laTsNs to get qPred
        var qPred = base.q.copyOf()
        var lastTs = base.ts
        var spanMsAcc = 0.0

        val git = gyroBuf.iterator()
        while (git.hasNext()) {
            val g = git.next()
            if (g.ts <= base.ts) continue
            if (g.ts > laTsNs) break
            val dt = ((g.ts - lastTs).coerceAtLeast(0L)).toDouble() * 1e-9
            if (dt > 0.0) {
                val dq = OrientationUtils.fromOmegaDt(g.wx.toDouble(), g.wy.toDouble(), g.wz.toDouble(), dt)
                qPred = OrientationUtils.mul(dq, qPred)
                OrientationUtils.normalize(qPred)
                lastTs = g.ts
                spanMsAcc += dt * 1000.0
            }
        }
        // Tail from last gyro to laTsNs: use last known gyro if any
        val dtTail = ((laTsNs - lastTs).coerceAtLeast(0L)).toDouble() * 1e-9
        if (dtTail > 0.0) {
            val last = gyroBuf.lastOrNull()
            val wx = last?.wx?.toDouble() ?: 0.0
            val wy = last?.wy?.toDouble() ?: 0.0
            val wz = last?.wz?.toDouble() ?: 0.0
            val dqTail = OrientationUtils.fromOmegaDt(wx, wy, wz, dtTail)
            qPred = OrientationUtils.mul(dqTail, qPred)
            OrientationUtils.normalize(qPred)
            spanMsAcc += dtTail * 1000.0
        }

        // 3) rotate LA to world (ENU) and project onto forward
        val laWorld = OrientationUtils.rotate(qPred, ax, ay, az) // (East, North, Up)
        val pPred = laWorld[0].toDouble() * dirFx + laWorld[1].toDouble() * dirFy

        // velocity integration uses dt from caller (we’ll add via updateVelocity)
        // Here we pass back pPred; service will integrate with its dtSec to keep consistency.
        val ypr = OrientationUtils.toYawPitchRollDeg(qPred)
        return PredResult(
            pPred = pPred,
            vPred = 0.0, // placeholder, service will add dt * pPred to a local accumulator
            oriTsUsed = base.ts,
            oriAgeSec = spanNs * 1e-9,
            gyroSpanMs = spanMsAcc,
            yawDeg = ypr.first, pitchDeg = ypr.second, rollDeg = ypr.third
        )
    }

    fun addToVelocity(dtSec: Double, pPred: Double): Double {
        if (dtSec > 0.0) vAccum += dtSec * pPred
        return vAccum
    }
}
