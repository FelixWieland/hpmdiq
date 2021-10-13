from hpmdiq import InfluxDBClient
import timeit

# create a connection
conn = InfluxDBClient(
    "http://localhost:8086",
    "EqYleExLjXglQfI12C6aDfDLSvugnsJ9ELDZlGcAPJ7RnY_o-kt9tMv1YIXDMksXcVYynb6Jvn06nzBr3o47jw==",
    "hpmdiq",
)


def query_vec():
    res = conn.query_vec(
        """
        from(bucket: "data")
        |> range(start: -7d, stop: now())
    """
    )
    return res


def query_raw():
    res = conn.query_raw(
        """
        from(bucket: "data")
        |> range(start: -7d, stop: now())
    """
    )
    return res


(header, result) = query_vec()
print(header, result[len(result) - 1])

print("query_vec", timeit.timeit("query_vec()", globals=locals(), number=1), "s")
print("query_raw", timeit.timeit("query_raw()", globals=locals(), number=1), "s")
