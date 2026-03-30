import time
from .sketches.reservoir_sampling import reservoir_sample
from .sketches.count_min_sketch import CountMinSketch
from .sketches.hll_wrapper import hll_count_distinct


# -----------------------------------
# HELPER: Map accuracy → parameters
# -----------------------------------

def get_parameters(df, accuracy):
    n = len(df)

    # Sample size increases with accuracy
    sample_size = max(50, int(n * accuracy))

    # HLL precision (4–16)
    precision = int(4 + accuracy * 12)

    return sample_size, precision


# -----------------------------------
# COUNT (using Count-Min Sketch)
# -----------------------------------

def approx_count(df, column):
    cms = CountMinSketch()

    for val in df[column]:
        cms.add(val)

    # Return total count (approx)
    return len(df)


# -----------------------------------
# COUNT DISTINCT (HyperLogLog)
# -----------------------------------

def approx_count_distinct(df, column, precision):
    return hll_count_distinct(df[column], precision)


# -----------------------------------
# SUM & AVG (Reservoir Sampling)
# -----------------------------------

def approx_sum_avg(df, column, sample_size):
    sample = reservoir_sample(df, sample_size)

    sample_sum = sample[column].sum()
    sample_avg = sample[column].mean()

    scale = len(df) / sample_size

    approx_sum = sample_sum * scale

    return approx_sum, sample_avg


# -----------------------------------
# GROUP BY (Sampling)
# -----------------------------------

def approx_group_by(df, group_col, agg_col, sample_size):
    sample = reservoir_sample(df, sample_size)

    grouped = sample.groupby(group_col)[agg_col].agg(['sum', 'count'])

    scale = len(df) / sample_size

    result = {}

    for key, row in grouped.iterrows():
        approx_sum = row['sum'] * scale
        approx_avg = approx_sum / (row['count'] * scale)

        result[str(key)] = {
            "sum": round(approx_sum, 2),
            "avg": round(approx_avg, 2)
        }

    return result


# -----------------------------------
# MAIN ENGINE FUNCTION
# -----------------------------------

def run_approx(query, df, accuracy=0.9):
    start = time.time()
    query = query.lower()

    sample_size, precision = get_parameters(df, accuracy)

    try:
        # COUNT DISTINCT
        if "count(distinct" in query:
            col = query.split("count(distinct")[1].split(")")[0].strip()
            result = approx_count_distinct(df, col, precision)

        # COUNT
        elif "count(" in query:
            result = approx_count(df, df.columns[0])

        # SUM
        elif "sum(" in query:
            col = query.split("sum(")[1].split(")")[0].strip()
            result, _ = approx_sum_avg(df, col, sample_size)

        # AVG
        elif "avg(" in query:
            col = query.split("avg(")[1].split(")")[0].strip()
            _, result = approx_sum_avg(df, col, sample_size)

        # GROUP BY
        elif "group by" in query:
            parts = query.split("group by")
            group_col = parts[1].strip()
            agg_col = query.split("select")[1].split("from")[0].split(",")[1].strip()

            result = approx_group_by(df, group_col, agg_col, sample_size)

        else:
            result = "Unsupported query"

    except Exception as e:
        result = f"Error: {str(e)}"

    end = time.time()

    return {
        "result": result,
        "time": round(end - start, 4),
        "sample_size": sample_size,
        "accuracy": accuracy
    }