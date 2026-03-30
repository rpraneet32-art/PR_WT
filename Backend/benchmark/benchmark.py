def calculate_metrics(exact_val, approx_val, exact_time_ms, approx_time_ms):
    """
    Takes the results and times from both engines and calculates the trade-offs.
    Now safely handles GROUP BY dictionary results!
    """
    # 1. Calculate the Error Percentage
    if isinstance(exact_val, dict) and isinstance(approx_val, dict):
        # GROUP BY Math
        errors = []
        for key in exact_val:
            if key in approx_val and exact_val[key] != 0:
                err = abs(exact_val[key] - approx_val[key]) / abs(exact_val[key]) * 100
                errors.append(err)
        error_pct = sum(errors) / len(errors) if errors else 0.0
    else:
        # Standard Numeric Math
        try:
            e_val = float(exact_val)
            a_val = float(approx_val)
            if e_val != 0:
                error_pct = abs(e_val - a_val) / e_val * 100
            else:
                error_pct = 0.0
        except (TypeError, ValueError):
            return {"error": "Invalid data format for metric calculation."}

    accuracy_pct = max(0.0, 100.0 - error_pct)
    
    # 2. Calculate the Speedup Multiplier
    if approx_time_ms > 0:
        speedup = exact_time_ms / approx_time_ms
    else:
        speedup = 0.0 

    return {
        "exact_time_ms": round(exact_time_ms, 4),
        "approx_time_ms": round(approx_time_ms, 4),
        "error_percentage": round(error_pct, 4),
        "accuracy_percentage": round(accuracy_pct, 2),
        "speedup_x": round(speedup, 2),
        "met_3x_target": speedup >= 3.0  
    }