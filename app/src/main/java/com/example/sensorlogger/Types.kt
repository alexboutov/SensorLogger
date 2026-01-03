package com.example.sensorlogger

// Sensor samples (monotonic ns)
data class GyroSample(val ts: Long, val wx: Float, val wy: Float, val wz: Float)
data class OriSample(val ts: Long, val q: FloatArray) // quaternion [w,x,y,z]

// Engine result at a given LA timestamp
data class PredResult(
    val pPred: Double,           // projected acceleration along forward
    val vPred: Double,           // integrated velocity (engine-local)
    val oriTsUsed: Long,         // orientation timestamp used for prediction
    val oriAgeSec: Double,       // (laTs - oriTs) seconds
    val gyroSpanMs: Double,      // total ms of gyro integration applied
    val yawDeg: Double,          // yaw of qPred
    val pitchDeg: Double,
    val rollDeg: Double
)
