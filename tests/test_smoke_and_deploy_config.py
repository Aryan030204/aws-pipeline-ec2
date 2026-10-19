"""Import smoke test for every module + consistency checks on the deploy files,
so a broken import or a compose entry pointing at a missing Dockerfile fails CI
before anything is deployed."""
import importlib
import os
import re
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(name):
    with open(os.path.join(ROOT, name), encoding="utf-8") as fh:
        return fh.read()


class ImportSmokeTests(unittest.TestCase):
    def test_all_modules_import(self):
        import aws_background  # noqa: F401  (must come first: circular import order)

        names = ["bundle_rollups"]
        for pkg in ("pipeline", "workers"):
            for f in sorted(os.listdir(os.path.join(ROOT, pkg))):
                if f.endswith(".py") and f != "__init__.py":
                    names.append(f"{pkg}.{f[:-3]}")
        for name in names:
            with self.subTest(module=name):
                importlib.import_module(name)


class DeployConfigTests(unittest.TestCase):
    def test_compose_dockerfiles_exist_and_names_unique(self):
        compose = _read("docker-compose.yml")
        dockerfiles = re.findall(r"dockerfile:\s*(\S+)", compose)
        self.assertTrue(dockerfiles)
        for d in dockerfiles:
            self.assertTrue(os.path.isfile(os.path.join(ROOT, d)), d)
        names = re.findall(r"container_name:\s*(\S+)", compose)
        self.assertEqual(len(names), len(set(names)))

    def test_every_worker_has_a_compose_service(self):
        from workers import registry

        compose = _read("docker-compose.yml")
        for spec in registry.WORKERS.values():
            with self.subTest(worker=spec.name):
                self.assertIn(f"{spec.name}-worker" if not spec.name.endswith("worker") else spec.name, compose)

    def test_requirements_cover_third_party_imports(self):
        reqs = _read("requirements.txt").lower()
        for pkg in ("flask", "apscheduler", "pymongo", "sqlalchemy", "mysql-connector-python", "cryptography"):
            self.assertIn(pkg, reqs)


if __name__ == "__main__":
    unittest.main()
