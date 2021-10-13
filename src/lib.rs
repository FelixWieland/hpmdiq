use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass]
struct InfluxDBClient {
    #[pyo3(get)]
    url: String,
    #[pyo3(get)]
    token: String,
    #[pyo3(get)]
    org: String,
}

#[pymethods]
impl InfluxDBClient {
    #[new]
    pub fn new(url: String, token: String, org: String) -> Self {
        pyo3::prepare_freethreaded_python();
        InfluxDBClient { url, token, org }
    }

    pub fn query_raw(&self, query: String) -> PyResult<(Vec<String>, String)> {
        let client = reqwest::blocking::Client::new();
        let res = client
            .post(&format!("{}/api/v2/query?org={}", &self.url, &self.org))
            .header("Authorization", &format!("Token {}", &self.token))
            .header("Accept", "application/csv")
            .header("Content-type", "application/vnd.flux")
            .header("Accept-Encoding", "gzip")
            .body(query)
            .send();
        match res {
            Ok(response) => match response.text() {
                Ok(str) => {
                    let index = str.find("\r\n");
                    match index {
                        Some(i) => {
                            if str.len() > i + 2 {
                                let header = str.to_string()[..i]
                                                           .split(",")
                                                           .map(|e| e.to_owned())
                                                           .collect();
                                let body = str.to_string()[i + 2..].to_string();
                                Ok((header, body))
                            } else {
                                Err(PyValueError::new_err("no data"))
                            }
                        }
                        None => Err(PyValueError::new_err("no data")),
                    }
                }
                Err(err) => Err(PyValueError::new_err(err.to_string())),
            },
            Err(err) => Err(PyValueError::new_err(err.to_string())),
        }
    }

    pub fn query_vec(&self, query: String) -> PyResult<(Vec<String>, Vec<Vec<String>>)> {
        let (header, res_raw) = self.query_raw(query)?;
        let lines: Vec<Vec<String>> = res_raw.split("\r\n")
                                             .map(|e| e.split(",").map(|r| r.to_owned()).collect())
                                             .filter(|e: &Vec<String>| e.len() > 1)
                                             .collect();
        Ok((header, lines))
    }
}

#[pymodule]
fn hpmdiq(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<InfluxDBClient>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_raw() {
        let conn = InfluxDBClient::new("http://localhost:8086".to_owned(), "EqYleExLjXglQfI12C6aDfDLSvugnsJ9ELDZlGcAPJ7RnY_o-kt9tMv1YIXDMksXcVYynb6Jvn06nzBr3o47jw==".to_owned(), "hpmdiq".to_owned());
        let query = "
            from(bucket: \"data\")
            |> range(start: -7d, stop: now())
        "
        .to_owned();
        let r = conn.query_raw(query);
        match r {
            Ok((header, body)) => {
                let l = body.len();
                println!("{}", header.join(";"));
                assert!(l > 0);
            }
            Err(err) => {
                println!("{}", err)
            }
        }
    }

    #[test]
    fn test_query_vec() {
        let conn = InfluxDBClient::new("http://localhost:8086".to_owned(), "EqYleExLjXglQfI12C6aDfDLSvugnsJ9ELDZlGcAPJ7RnY_o-kt9tMv1YIXDMksXcVYynb6Jvn06nzBr3o47jw==".to_owned(), "hpmdiq".to_owned());
        let query = "
            from(bucket: \"data\")
            |> range(start: -7d, stop: now())
        "
        .to_owned();
        let r = conn.query_vec(query);
        match r {
            Ok((header, body)) => {
                let l = body.len();
                println!("{}", header.join(";"));
                println!("{}", body.last().unwrap().join(","));
                assert!(l > 0);
            }
            Err(err) => {
                println!("{}", err)
            }
        }
    }
}