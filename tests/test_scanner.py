import math
import unittest

from scanner import Alert, crossing, ema, macd, group_messages, format_utc


class ScannerTests(unittest.TestCase):
    def test_ema_known_series(self):
        self.assertEqual(ema([10, 12, 14], 3), [10, 11, 12.5])

    def test_macd_line_is_ema_12_minus_ema_26(self):
        closes = list(range(100, 250))
        line, signal = macd(closes)
        self.assertEqual(len(line), len(closes))
        self.assertEqual(len(signal), len(closes))
        self.assertAlmostEqual(line[0], 0)
        self.assertGreater(line[-1], 0)

    def test_crossings_are_only_zero_line_not_signal_line(self):
        self.assertEqual(crossing(-0.001, 0.002), "LONG")
        self.assertEqual(crossing(0.001, -0.002), "SHORT")
        self.assertIsNone(crossing(-0.003, -0.001))
        self.assertIsNone(crossing(0.003, 0.001))
        self.assertEqual(crossing(0.0, 0.1), "LONG")

    def test_message_splitting(self):
        alerts = [Alert(f"COIN{i}USDT", "5m", "LONG", 1800000000000, -0.001, 0.002)
                  for i in range(100)]
        messages = group_messages(alerts)
        self.assertGreater(len(messages), 1)
        self.assertTrue(all(len(msg) <= 3500 for msg in messages))
        self.assertIn("COIN99USDT", "\n".join(messages))

    def test_close_time_is_utc(self):
        self.assertIn("UTC", format_utc(1800000000000))


if __name__ == "__main__":
    unittest.main()

# Add synthetic scanner integration checks without any network access.
class ScannerNetworkMockTests(unittest.TestCase):
    def test_first_run_baselines_no_alerts_and_state_dedups(self):
        from unittest.mock import patch
        import scanner
        interval_ms = 5 * 60_000
        begin = 1_800_000_000_000
        # A rising series yields MACD above zero after warmup.
        rows = [[begin + i*interval_ms, "1", "1", "1", str(100+i),
                 "1", begin + (i+1)*interval_ms-1] for i in range(210)]
        server_ms = begin + 211*interval_ms
        with patch.object(scanner, "binance_get", return_value=rows):
            alerts, checkpoint, status = scanner.scan_symbol("BTCUSDT", "5m", server_ms, None)
            self.assertEqual(alerts, [])
            self.assertEqual(status, "bootstrapped")
            self.assertEqual(checkpoint, begin + 210*interval_ms)
            alerts, checkpoint2, status = scanner.scan_symbol("BTCUSDT", "5m", server_ms, checkpoint)
            self.assertEqual(alerts, [])
            self.assertEqual(checkpoint, checkpoint2)
            self.assertEqual(status, "ok")
