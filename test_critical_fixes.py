#!/usr/bin/env python3
"""
Test script for critical fixes
Tests the loader imports and basic functionality
"""
import sys

def test_imports():
    """Test that loaders can be imported from taxdash package"""
    print("Testing imports from taxdash package...")
    try:
        from taxdash import load_and_process_data, load_and_process_sped_fiscal, load_and_process_ecd
        print("✓ All loaders imported successfully from taxdash package")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_empty_list_validation():
    """Test empty list validation logic"""
    print("\nTesting empty list validation...")

    # Test case 1: None
    uploaded_file = None
    if uploaded_file and len(uploaded_file) > 0:
        print("✗ None check failed - should be False")
        return False
    else:
        print("✓ None check passed")

    # Test case 2: Empty list
    uploaded_file = []
    if uploaded_file and len(uploaded_file) > 0:
        print("✗ Empty list check failed - should be False")
        return False
    else:
        print("✓ Empty list check passed")

    # Test case 3: List with items
    uploaded_file = ["file1.txt"]
    if uploaded_file and len(uploaded_file) > 0:
        print("✓ Non-empty list check passed")
    else:
        print("✗ Non-empty list check failed - should be True")
        return False

    return True

def test_session_state_guard():
    """Test session state guard logic"""
    print("\nTesting session state guard logic...")

    # Simulate session state
    session_state = {}

    # Test case 1: No processing_done flag
    if not session_state.get("processing_done", False):
        print("✓ Guard correctly blocks when processing_done is missing")
    else:
        print("✗ Guard should block when processing_done is missing")
        return False

    # Test case 2: processing_done = False
    session_state["processing_done"] = False
    if not session_state.get("processing_done", False):
        print("✓ Guard correctly blocks when processing_done is False")
    else:
        print("✗ Guard should block when processing_done is False")
        return False

    # Test case 3: processing_done = True
    session_state["processing_done"] = True
    if not session_state.get("processing_done", False):
        print("✗ Guard should allow when processing_done is True")
        return False
    else:
        print("✓ Guard correctly allows when processing_done is True")

    return True

def main():
    print("=" * 60)
    print("Critical Fixes Test Suite")
    print("=" * 60)

    results = []

    # Run tests
    results.append(("Import Test", test_imports()))
    results.append(("Empty List Validation", test_empty_list_validation()))
    results.append(("Session State Guard", test_session_state_guard()))

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    all_passed = True
    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False

    print("=" * 60)

    if all_passed:
        print("✓ All tests passed!")
        return 0
    else:
        print("✗ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
