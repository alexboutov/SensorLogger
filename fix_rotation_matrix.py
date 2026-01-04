#!/usr/bin/env python3
"""
Fix rotation matrix indexing in SensorLoggerService.kt

The bug: Matrix is accessed by columns (R^T) instead of rows (R)
This causes device->world transform to be inverted.

Usage:
  python fix_rotation_matrix.py path_to_SensorLoggerService.kt
"""

import sys

def fix_rotation_matrix(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    changes = []
    
    # Fix 1: GM projection (lines ~475-476)
    old_gm = '''// GM -> world (use R^T)
                            val accWorldX_gm = rotMatrixGM[0]*laX + rotMatrixGM[3]*laY + rotMatrixGM[6]*laZ
                            val accWorldY_gm = rotMatrixGM[1]*laX + rotMatrixGM[4]*laY + rotMatrixGM[7]*laZ'''
    
    new_gm = '''// GM -> world (R * device_vector, row-major access)
                            val accWorldX_gm = rotMatrixGM[0]*laX + rotMatrixGM[1]*laY + rotMatrixGM[2]*laZ
                            val accWorldY_gm = rotMatrixGM[3]*laX + rotMatrixGM[4]*laY + rotMatrixGM[5]*laZ'''
    
    if old_gm in content:
        content = content.replace(old_gm, new_gm)
        changes.append("Fixed GM projection (rotMatrixGM indexing)")
    else:
        print("WARNING: Could not find GM projection pattern to fix")
    
    # Fix 2: RV projection (lines ~482-483)
    old_rv = '''// RV -> world (use R^T)
                            val accWorldX_rv = rotMatrixRV[0]*laX + rotMatrixRV[3]*laY + rotMatrixRV[6]*laZ
                            val accWorldY_rv = rotMatrixRV[1]*laX + rotMatrixRV[4]*laY + rotMatrixRV[7]*laZ'''
    
    new_rv = '''// RV -> world (R * device_vector, row-major access)
                            val accWorldX_rv = rotMatrixRV[0]*laX + rotMatrixRV[1]*laY + rotMatrixRV[2]*laZ
                            val accWorldY_rv = rotMatrixRV[3]*laX + rotMatrixRV[4]*laY + rotMatrixRV[5]*laZ'''
    
    if old_rv in content:
        content = content.replace(old_rv, new_rv)
        changes.append("Fixed RV projection (rotMatrixRV indexing)")
    else:
        print("WARNING: Could not find RV projection pattern to fix")
    
    # Fix 3: ACC-g projection (lines ~497-498)
    old_acc = '''val linWorldX = rotMatrixGM[0]*linDevX + rotMatrixGM[3]*linDevY + rotMatrixGM[6]*linDevZ // East
                            val linWorldY = rotMatrixGM[1]*linDevX + rotMatrixGM[4]*linDevY + rotMatrixGM[7]*linDevZ // North'''
    
    new_acc = '''val linWorldX = rotMatrixGM[0]*linDevX + rotMatrixGM[1]*linDevY + rotMatrixGM[2]*linDevZ // East
                            val linWorldY = rotMatrixGM[3]*linDevX + rotMatrixGM[4]*linDevY + rotMatrixGM[5]*linDevZ // North'''
    
    if old_acc in content:
        content = content.replace(old_acc, new_acc)
        changes.append("Fixed ACC-g projection (linWorld indexing)")
    else:
        print("WARNING: Could not find ACC-g projection pattern to fix")
    
    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print("SUCCESS: Modified " + filepath)
        print("Changes made:")
        for c in changes:
            print("  - " + c)
    else:
        print("No changes made - patterns not found or already fixed")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fix_rotation_matrix.py <path_to_SensorLoggerService.kt>")
        sys.exit(1)
    
    fix_rotation_matrix(sys.argv[1])
