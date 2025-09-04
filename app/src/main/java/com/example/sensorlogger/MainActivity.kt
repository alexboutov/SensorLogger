package com.example.sensorlogger

import android.content.Intent
import android.os.Bundle
import android.view.inputmethod.EditorInfo
import androidx.appcompat.app.AppCompatActivity
import com.example.sensorlogger.databinding.ActivityMainBinding

private const val PREFS_NAME = "sensor_logger_prefs"
private const val PREF_AZIMUTH = "azimuth_deg"
const val EXTRA_AZIMUTH_DEG = "com.example.sensorlogger.EXTRA_AZIMUTH_DEG"

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val prefs by lazy { getSharedPreferences(PREFS_NAME, MODE_PRIVATE) }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // 1) Restore last saved azimuth
        val last = prefs.getString(PREF_AZIMUTH, "0.0") ?: "0.0"
        binding.azimuthEditText.setText(last)

        // 2) Save when user taps "Done" on the keyboard
        binding.azimuthEditText.setOnEditorActionListener { _, actionId, _ ->
            if (actionId == EditorInfo.IME_ACTION_DONE) {
                saveAzimuth()
                true
            } else false
        }

        // 3) START: save + normalize and pass to service
        binding.startButton.setOnClickListener {
            saveAzimuth()
            val azText = binding.azimuthEditText.text?.toString()?.trim().orEmpty()
            val azimuthDeg = azText.toDoubleOrNull()?.let { v ->
                // normalize into [0, 360)
                val mod = ((v % 360.0) + 360.0) % 360.0
                mod
            } ?: 0.0

            val intent = Intent(this, SensorLoggerService::class.java).apply {
                putExtra(EXTRA_AZIMUTH_DEG, azimuthDeg)
            }
            // Use the same start call you already use (foreground or not)
            startService(intent)
        }

        // 4) STOP remains as you have it
        binding.stopButton.setOnClickListener {
            stopService(Intent(this, SensorLoggerService::class.java))
        }
    }

    override fun onPause() {
        super.onPause()
        // 5) Save on lifecycle exit as well
        saveAzimuth()
    }

    private fun saveAzimuth() {
        val azText = binding.azimuthEditText.text?.toString()?.trim().orEmpty()
        prefs.edit().putString(PREF_AZIMUTH, azText).apply()
    }
}
