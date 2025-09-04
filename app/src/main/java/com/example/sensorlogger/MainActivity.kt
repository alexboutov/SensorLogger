package com.example.sensorlogger

import android.content.Intent
import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import com.example.sensorlogger.databinding.ActivityMainBinding

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.startButton.setOnClickListener {
            startService(Intent(this, SensorLoggerService::class.java))
        }

        binding.stopButton.setOnClickListener {
            stopService(Intent(this, SensorLoggerService::class.java))
        }
    }
}