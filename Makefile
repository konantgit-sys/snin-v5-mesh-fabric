# SNIN V5 Mesh Fabric — Makefile
# ================================

PYTHON = python3
DIR = /home/agent/data/sites/relay-mesh

.PHONY: test test-middleware test-phase1 test-all lint clean

# === UNIT TESTS ===

test: test-middleware test-phase12  ## Запустить все тесты
	@echo ""
	@echo "🎯 All tests complete"

test-middleware:  ## Middleware (Phase 4) — 53 tests
	@echo "--- Middleware Test Suite ---"
	cd $(DIR) && $(PYTHON) test_middleware.py
	@echo ""

test-phase12:  ## Phase 1-2 — интеграционные тесты
	@echo "--- Phase 1-2 Integration Tests ---"
	cd $(DIR) && $(PYTHON) test_phase1_2.py
	@echo ""

# === HEALTH CHECKS ===

health:  ## Проверить все health-endpoints
	@echo "--- Service Health ---"
	@cd $(DIR) && $(PYTHON) health_check_engine.py --check-only 2>/dev/null || \
		echo "⚠️ health engine not running, checking ports..."
	@for port in 8080 9932 9910 9920 9931 9940 9916 9915 9999 9970; do \
		(echo >/dev/tcp/127.0.0.1/$$port) 2>/dev/null && \
		echo "  ✅ :$$port alive" || echo "  ❌ :$$port dead"; \
	done

# === SYNTAX CHECK ===

lint:  ## Проверить синтаксис всех Python файлов
	@cd $(DIR) && for f in *.py; do \
		$(PYTHON) -c "import py_compile; py_compile.compile('$$f', doraise=True)" 2>/dev/null && \
			echo "  ✅ $$f" || echo "  ❌ $$f"; \
	done

# === CLEANUP ===

clean:  ## Удалить временные файлы
	@rm -f $(DIR)/*.pyc $(DIR)/__pycache__/ -rf
	@rm -f /home/agent/data/logs/*.tmp
	@echo "🧹 Cleaned"

# === HELP ===

help:
	@echo "SNIN V5 Mesh Fabric — Makefile"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
	@echo ""
	@echo "Quick:  make test"
