package com.example.sensorlogger

import android.hardware.SensorManager
import kotlin.math.*

object OrientationUtils {
    // ===== Quaternion utilities =====
    // All quats are [w,x,y,z]
    fun normalize(q: FloatArray) {
        var s = 0.0
        for (i in 0..3) s += (q[i] * q[i]).toDouble()
        val inv = 1.0 / sqrt(s.coerceAtLeast(1e-24))
        for (i in 0..3) q[i] = (q[i] * inv).toFloat()
    }

    fun mul(a: FloatArray, b: FloatArray, out: FloatArray = FloatArray(4)): FloatArray {
        val aw=a[0]; val ax=a[1]; val ay=a[2]; val az=a[3]
        val bw=b[0]; val bx=b[1]; val by=b[2]; val bz=b[3]
        out[0] = aw*bw - ax*bx - ay*by - az*bz
        out[1] = aw*bx + ax*bw + ay*bz - az*by
        out[2] = aw*by - ax*bz + ay*bw + az*bx
        out[3] = aw*bz + ax*by - ay*bx + az*bw
        return out
    }

    fun fromOmegaDt(wx: Double, wy: Double, wz: Double, dt: Double): FloatArray {
        val theta = sqrt(wx*wx + wy*wy + wz*wz) * dt
        if (theta < 1e-6) {
            return floatArrayOf(
                1f,
                (0.5 * wx * dt).toFloat(),
                (0.5 * wy * dt).toFloat(),
                (0.5 * wz * dt).toFloat()
            ).also { normalize(it) }
        }
        val half = 0.5 * theta
        val s = sin(half) / theta
        return floatArrayOf(
            cos(half).toFloat(),
            (s * wx * dt).toFloat(),
            (s * wy * dt).toFloat(),
            (s * wz * dt).toFloat()
        )
    }

    fun rotate(q: FloatArray, vx: Float, vy: Float, vz: Float): FloatArray {
        val w=q[0]; val x=q[1]; val y=q[2]; val z=q[3]
        val t0 = -x*vx - y*vy - z*vz
        val t1 =  w*vx + y*vz - z*vy
        val t2 =  w*vy - x*vz + z*vx
        val t3 =  w*vz + x*vy - y*vx
        val rx = -t0*x + t1*w - t2*z + t3*y
        val ry = -t0*y + t2*w - t3*x + t1*z
        val rz = -t0*z + t3*w - t1*y + t2*x
        return floatArrayOf(rx, ry, rz)
    }

    fun fromRotationMatrix(R: FloatArray): FloatArray {
        val tr = (R[0] + R[4] + R[8]).toDouble()
        val q = FloatArray(4)
        if (tr > 0) {
            val s = kotlin.math.sqrt(tr + 1.0) * 2.0
            q[0] = (0.25 * s).toFloat()
            q[1] = ((R[7] - R[5]) / s).toFloat()
            q[2] = ((R[2] - R[6]) / s).toFloat()
            q[3] = ((R[3] - R[1]) / s).toFloat()
        } else if ((R[0] > R[4]) && (R[0] > R[8])) {
            val s = kotlin.math.sqrt(1.0 + R[0] - R[4] - R[8]) * 2.0
            q[0] = ((R[7] - R[5]) / s).toFloat()
            q[1] = (0.25 * s).toFloat()
            q[2] = ((R[1] + R[3]) / s).toFloat()
            q[3] = ((R[2] + R[6]) / s).toFloat()
        } else if (R[4] > R[8]) {
            val s = kotlin.math.sqrt(1.0 + R[4] - R[0] - R[8]) * 2.0
            q[0] = ((R[2] - R[6]) / s).toFloat()
            q[1] = ((R[1] + R[3]) / s).toFloat()
            q[2] = (0.25 * s).toFloat()
            q[3] = ((R[5] + R[7]) / s).toFloat()
        } else {
            val s = kotlin.math.sqrt(1.0 + R[8] - R[0] - R[4]) * 2.0
            q[0] = ((R[3] - R[1]) / s).toFloat()
            q[1] = ((R[2] + R[6]) / s).toFloat()
            q[2] = ((R[5] + R[7]) / s).toFloat()
            q[3] = (0.25 * s).toFloat()
        }
        normalize(q)
        return q
    }

    fun fromRotVec(rv: FloatArray): FloatArray {
        val R = FloatArray(9)
        SensorManager.getRotationMatrixFromVector(R, rv)
        return fromRotationMatrix(R)
    }
    fun conjugate(q: FloatArray): FloatArray = floatArrayOf(q[0], -q[1], -q[2], -q[3])  // assumes [w,x,y,z]

    fun toYawPitchRollDeg(q: FloatArray): Triple<Double, Double, Double> {
        val w=q[0]; val x=q[1]; val y=q[2]; val z=q[3]
        val xx=x*x; val yy=y*y; val zz=z*z
        val wx=w*x; val wy=w*y; val wz=w*z
        val xy=x*y; val xz=x*z; val yz=y*z
        val R = FloatArray(9)
        R[0] = 1f - 2f*(yy+zz); R[1] = 2f*(xy - wz);   R[2] = 2f*(xz + wy)
        R[3] = 2f*(xy + wz);    R[4] = 1f - 2f*(xx+zz);R[5] = 2f*(yz - wx)
        R[6] = 2f*(xz - wy);    R[7] = 2f*(yz + wx);   R[8] = 1f - 2f*(xx+yy)
        val vals = FloatArray(3)
        SensorManager.getOrientation(R, vals)
        return Triple(
            Math.toDegrees(vals[0].toDouble()),
            Math.toDegrees(vals[1].toDouble()),
            Math.toDegrees(vals[2].toDouble())
        )
    }
}
