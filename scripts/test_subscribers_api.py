#!/usr/bin/env python3
import sys

sys.path.insert(0, "server")
from app import app


def main():
  client = app.test_client()

  health = client.get("/api/health")
  assert health.status_code == 200
  assert health.json.get("ok") is True

  bad = client.post("/api/subscribers", json={"email": "bad"})
  assert bad.status_code == 400

  good = client.post("/api/subscribers", json={"email": "apitest_subscriber@example.com"})
  assert good.status_code == 200
  assert good.json.get("ok") is True

  listed = client.get("/api/subscribers")
  assert listed.status_code == 200
  assert any(x.get("email") == "apitest_subscriber@example.com" for x in listed.json.get("subscribers", []))

  print("PASS: /api/subscribers API")


if __name__ == "__main__":
  main()
