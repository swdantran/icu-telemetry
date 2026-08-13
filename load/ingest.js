import http from "k6/http";

export const options = {
  scenarios: {
    ingest: { executor: "constant-arrival-rate", rate: 2000, timeUnit: "1s",
              duration: "60s", preAllocatedVUs: 100, maxVUs: 800 },
  },
};

export default function () {
  const body = JSON.stringify({
    patient_id: `p${(__ITER % 30) + 1}`,
    hr: 60 + Math.random() * 40, spo2: 94 + Math.random() * 5,
    bp_sys: 100 + Math.random() * 40, bp_dia: 60 + Math.random() * 30,
    resp_rate: 10 + Math.random() * 8,
  });
  http.post("http://localhost:8000/readings", body,
            { headers: { "Content-Type": "application/json" } });
}