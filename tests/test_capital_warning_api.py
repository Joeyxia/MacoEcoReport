import sys
import unittest

sys.path.insert(0, ".")

from server.app import app


class CapitalWarningApiTest(unittest.TestCase):
  def setUp(self):
    app.config.update(TESTING=True)
    self.client = app.test_client()
    with self.client.session_transaction() as sess:
      sess["public_user"] = {"email": "xiayiping@gmail.com", "loginAt": "2026-03-17T00:00:00Z"}

  def test_capital_warning_endpoints(self):
    self.assertEqual(self.client.get("/api/regime/latest").status_code, 200)
    self.assertEqual(self.client.get("/api/regime/history?days=30").status_code, 200)
    self.assertEqual(self.client.get("/api/alerts/latest").status_code, 200)
    self.assertEqual(self.client.get("/api/geopolitical-overlay/latest").status_code, 200)
    self.assertEqual(self.client.get("/api/transmission/latest").status_code, 200)
    self.assertEqual(self.client.get("/api/action-bias/latest").status_code, 200)

  def test_watchlist_crud(self):
    create = self.client.post(
      "/api/portfolio/watchlists",
      json={"user_email": "xiayiping@gmail.com", "list_name": "API Test List"},
    )
    self.assertIn(create.status_code, (200, 201))
    item = (create.get_json() or {}).get("item") or {}
    watchlist_id = item.get("id")
    self.assertTrue(watchlist_id)
    add = self.client.post(
      f"/api/portfolio/watchlists/{watchlist_id}/positions",
      json={"ticker": "PDD", "quantity": 10},
    )
    self.assertIn(add.status_code, (200, 201))
    self.assertEqual(self.client.get(f"/api/portfolio/watchlists/{watchlist_id}/positions").status_code, 200)
    self.assertEqual(self.client.get("/api/portfolio/risk-summary?user_email=xiayiping@gmail.com").status_code, 200)

  def test_stock_macro_endpoints(self):
    self.assertEqual(self.client.get("/api/stocks/PDD/macro-exposure").status_code, 200)
    self.assertEqual(self.client.get("/api/stocks/PDD/macro-signal/latest").status_code, 200)


if __name__ == "__main__":
  unittest.main()
