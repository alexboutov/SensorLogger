#!/usr/bin/env python3
"""
Disable logging of non-LA CSV files in SensorLoggerService.kt

This script comments out the writeSimple() calls for ACC, GRAV, MAG, ROTVEC
sensors, keeping only the LA (Linear Acceleration) CSV logging active.

Usage:
  python disable_extra_logs.py path_to_SensorLoggerService.kt
"""

import sys

def disable_extra_logs(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    changes = []
    
    # Fix 1: Disable ACC logging
    old_acc = '''Sensor.TYPE_ACCELEROMETER -> {
                        latestACC.ts = sensorTsNs
                        System.arraycopy(values, 0, latestACC.v, 0, 3)
                        writeSimple(sensorType, sensorTsNs, values)
                    }'''
    
    new_acc = '''Sensor.TYPE_ACCELEROMETER -> {
                        latestACC.ts = sensorTsNs
                        System.arraycopy(values, 0, latestACC.v, 0, 3)
                        // writeSimple(sensorType, sensorTsNs, values)  // Disabled - only logging LA
                    }'''
    
    if old_acc in content:
        content = content.replace(old_acc, new_acc)
        changes.append("Disabled ACC CSV logging")
    
    # Fix 2: Disable GRAVITY logging
    old_grav = '''Sensor.TYPE_GRAVITY -> {
                        writeSimple(sensorType, sensorTsNs, values)'''
    
    new_grav = '''Sensor.TYPE_GRAVITY -> {
                        // writeSimple(sensorType, sensorTsNs, values)  // Disabled - only logging LA'''
    
    if old_grav in content:
        content = content.replace(old_grav, new_grav)
        changes.append("Disabled GRAV CSV logging")
    
    # Fix 3: Disable MAGNETIC_FIELD logging
    old_mag = '''Sensor.TYPE_MAGNETIC_FIELD -> {
                        writeSimple(sensorType, sensorTsNs, values)'''
    
    new_mag = '''Sensor.TYPE_MAGNETIC_FIELD -> {
                        // writeSimple(sensorType, sensorTsNs, values)  // Disabled - only logging LA'''
    
    if old_mag in content:
        content = content.replace(old_mag, new_mag)
        changes.append("Disabled MAG CSV logging")
    
    # Fix 4: Disable ROTATION_VECTOR logging
    old_rv = '''Sensor.TYPE_ROTATION_VECTOR -> {
                        writeSimple(sensorType, sensorTsNs, values)'''
    
    new_rv = '''Sensor.TYPE_ROTATION_VECTOR -> {
                        // writeSimple(sensorType, sensorTsNs, values)  // Disabled - only logging LA'''
    
    if old_rv in content:
        content = content.replace(old_rv, new_rv)
        changes.append("Disabled ROTVEC CSV logging")
    
    # Fix 5: Disable combined CSV logging (start)
    old_combined = '''// ---- Combined snapshot CSV (unchanged) ----
                runCatching {
                    val cw = getCombinedWriter() ?: return@runCatching'''
    
    new_combined = '''// ---- Combined snapshot CSV (DISABLED) ----
                /* // Disabled - only logging LA
                runCatching {
                    val cw = getCombinedWriter() ?: return@runCatching'''
    
    if old_combined in content:
        content = content.replace(old_combined, new_combined)
        changes.append("Disabled combined CSV logging (start)")
    
    # Fix 6: Close the comment block after combined CSV section
    old_combined_end = '''.onFailure {
                    if (isLogging) android.util.Log.e("SensorLogger", "combined write failed", it)
                }
            }
        }
    }'''
    
    new_combined_end = '''.onFailure {
                    if (isLogging) android.util.Log.e("SensorLogger", "combined write failed", it)
                }
                */ // End disabled combined CSV block
            }
        }
    }'''
    
    if old_combined_end in content:
        content = content.replace(old_combined_end, new_combined_end)
        changes.append("Disabled combined CSV logging (end)")
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print("SUCCESS: Modified " + filepath)
        print("Changes made:")
        for c in changes:
            print("  - " + c)
        print("")
        print("Now only the LA CSV (*_la.csv) will be generated.")
    else:
        print("No changes made - patterns not found or already modified")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python disable_extra_logs.py <path_to_SensorLoggerService.kt>")
        sys.exit(1)
    
    disable_extra_logs(sys.argv[1])
