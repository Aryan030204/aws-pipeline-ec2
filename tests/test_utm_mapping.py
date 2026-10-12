def get_category(src):
    src = (src or "").strip().lower()
    category = "others"
    if not src or src == "none" or src == "null":
        category = "direct"
    elif (
        "instagram" in src
        or "facebook" in src
        or src == "ig"
        or src == "fb"
        or "insta" in src
        or "meta" in src
    ):
        category = "Meta"
    elif "google" in src:
        category = "Google"
    return category


import unittest

CASES = [
    ("fb", "Meta"),
    ("ig", "Meta"),
    ("facebook_ads", "Meta"),
    ("instagram_bio", "Meta"),
    ("insta_reel", "Meta"),
    ("meta_ads", "Meta"),
    ("Meta_Campaign", "Meta"),
    ("facebook", "Meta"),
    ("fbi", "others"),
    ("ignite", "others"),
    ("google_search", "Google"),
    ("none", "direct"),
    ("null", "direct"),
    ("", "direct"),
    ("tik_tok", "others"),
]


class UtmMappingTests(unittest.TestCase):
    def test_categories(self):
        for src, expected in CASES:
            with self.subTest(src=src):
                self.assertEqual(get_category(src), expected)


if __name__ == "__main__":
    unittest.main()
