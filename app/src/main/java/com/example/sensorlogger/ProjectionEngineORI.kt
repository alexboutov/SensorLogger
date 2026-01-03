package com.example.sensorlogger

/**
 * ORI-triggered projection with LA backfill (nearest-neighbor matching).
 *
 * - Called from the service via onGyro/onOri/onLa (same as LA engine).
 * - For each LA call we:
 *     1) Buffer the LA sample.
 *     2) Choose the orientation closest to laTs (prefer <=; else earliest >=).
 *     3) Choose the LA closest to that orientation ts (prefer <=; else earliest >=).
 *     4) Rotate that LA into ENU with the chosen orientation and project onto (dirFx, dirFy).
 * - Velocity integrates on orientation cadence inside this engine (no service changes required).
 *
 * Telemetry:
 *   oriTsNsUsed     -> orientation timestamp actually used
 *   oriAgeSec_pred  -> (t_ori - t_la_used) in seconds (can be ± small; we clamp to show >=0)
 *   gyroSpanMs_pred -> 0.0 (no gyro prediction here)
 *   yaw/pitch/roll  -> Euler angles of the orientation used
 */
class ProjectionEngineORI(
    private val bufferWindowSec: Double = 0.50  // keep ~500 ms of history
) {
    private data class LaSample(val ts: Long, val ax: Float, val ay: Float, val az: Float)

    private val laBuf = ArrayDeque<LaSample>()
    private val oriBuf = ArrayDeque<OriSample>()

    private var vAccum = 0.0
    private var prevOriTs: Long = 0L           // last orientation ts used for integration
    private var lastPredOriTs: Long = 0L       // orientation ts for the last prediction returned

    private val horizonNs: Long
        get() = (bufferWindowSec * 1e9).toLong()

    fun clear() {
        laBuf.clear()
        oriBuf.clear()
        vAccum = 0.0
        prevOriTs = 0L
        lastPredOriTs = 0L
    }

    fun onGyro(@Suppress("UNUSED_PARAMETER") s: GyroSample) {
        // No gyro prediction in ORI-triggered engine; keep API uniform with LA engine.
    }

    fun onOri(o: OriSample) {
        oriBuf.addLast(o)
        trim(o.ts)
    }

    fun onLa(
        laTsNs: Long,
        ax: Float, ay: Float, az: Float,
        dirFx: Double, dirFy: Double
    ): PredResult? {
        // 1) Buffer incoming LA
        laBuf.addLast(LaSample(laTsNs, ax, ay, az))
        trim(laTsNs)

        if (oriBuf.isEmpty() || laBuf.isEmpty()) return null

        // 2) Find orientation closest to laTs (prefer <=; else earliest >=)
        val ori = findOriClosestTo(laTsNs) ?: return null
        val tOri = ori.ts

        // 3) Find LA closest to tOri (prefer <=; else earliest >=)
        val la = findLaClosestTo(tOri) ?: return null

        // 4) Project LA (device) into world ENU using orientation at tOri
        // (use inverse quaternion to map device->world)
        val qDW = OrientationUtils.conjugate(ori.q)   // or OrientationUtils.inverse(ori.q)
        val laWorld = OrientationUtils.rotate(qDW, la.ax, la.ay, la.az)
        val pPred = laWorld[0].toDouble() * dirFx + laWorld[1].toDouble() * dirFy

        // Telemetry
        val laAgeSecSigned = (tOri - la.ts).toDouble() * 1e-9 // signed for debugging
        val laAgeSecClamped = kotlin.math.abs(laAgeSecSigned) // report non-negative for CSV
        val (yawDeg, pitchDeg, rollDeg) = OrientationUtils.toYawPitchRollDeg(ori.q)

        // Remember which orientation ts we just used so addToVelocity() can integrate on ORI cadence
        lastPredOriTs = tOri

        return PredResult(
            pPred = pPred,
            vPred = 0.0,                 // engine will integrate internally in addToVelocity
            oriTsUsed = tOri,
            oriAgeSec = laAgeSecClamped, // how far LA is from the ORI we used
            gyroSpanMs = 0.0,            // no prediction in this engine
            yawDeg = yawDeg,
            pitchDeg = pitchDeg,
            rollDeg = rollDeg
        )
    }

    /**
     * Integrate on ORI cadence (ignore dtSec provided by the service).
     * We use Δt = (lastPredOriTs - prevOriTs) when lastPredOriTs advances.
     */
    fun addToVelocity(@Suppress("UNUSED_PARAMETER") dtSec: Double, pPred: Double): Double {
        val t = lastPredOriTs
        if (t != 0L) {
            if (prevOriTs == 0L) {
                prevOriTs = t
            } else if (t > prevOriTs) {
                val dt = (t - prevOriTs).toDouble() * 1e-9
                vAccum += dt * pPred
                prevOriTs = t
            }
        }
        return vAccum
    }

    // ----------------- helpers -----------------

    private fun trim(nowTs: Long) {
        val h = horizonNs
        while (laBuf.isNotEmpty()  && (nowTs - laBuf.first().ts)  > h) laBuf.removeFirst()
        while (oriBuf.isNotEmpty() && (nowTs - oriBuf.first().ts) > h) oriBuf.removeFirst()
    }

    private fun findOriClosestTo(ts: Long): OriSample? {
        if (oriBuf.isEmpty()) return null
        var best: OriSample? = null
        var bestAfter: OriSample? = null

        // walk forward; buffers are chronological
        for (o in oriBuf) {
            if (o.ts <= ts) best = o else { bestAfter = o; break }
        }
        // prefer the last <= ts
        if (best != null) return best
        // else fall back to earliest >= ts
        return bestAfter ?: oriBuf.lastOrNull()
    }

    private fun findLaClosestTo(ts: Long): LaSample? {
        if (laBuf.isEmpty()) return null
        var best: LaSample? = null
        var bestAfter: LaSample? = null

        for (s in laBuf) {
            if (s.ts <= ts) best = s else { bestAfter = s; break }
        }
        // choose whichever is closer in time
        return when {
            best == null -> bestAfter
            bestAfter == null -> best
            else -> {
                val dBefore = ts - best.ts
                val dAfter  = bestAfter.ts - ts
                if (dBefore <= dAfter) best else bestAfter
            }
        }
    }
}
