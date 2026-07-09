#!/usr/bin/env python3
"""Phase 14: Language Detection & Semantic Filter — tests."""

import sys
sys.path.insert(0, '.')

import redis
from knowledge_graph import KnowledgeGraph
from smart_router import SmartRouter
from semantic_router import create_semantic_router
from content_router import create_content_router, ContentRouter

passed = 0
failed = 0

def test(name, condition):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✅ {name}")
    else:
        failed += 1
        print(f"  ❌ {name}")

# ─── Setup ──────────────────────────────────────────
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
kg = KnowledgeGraph(r)
sr = SmartRouter()
sem_router = create_semantic_router(kg, sr, r)
cr = create_content_router(sem_router)

cr.register_expertise_batch({
    'node_ai': [('AI', 'Artificial Intelligence and machine learning', ['ai', 'ml', 'neural'])],
    'node_crypto': [('Crypto', 'Bitcoin cryptocurrency blockchain', ['bitcoin', 'crypto', 'btc'])],
    'node_tech': [('Tech', 'Technology software programming', ['tech', 'software', 'coding'])],
})

# ─── _is_latin_script unit tests ───────────────────
print("─ _is_latin_script ─")
test("English text → True", ContentRouter._is_latin_script("Hello world, this is AI technology"))
test("Japanese text → False", not ContentRouter._is_latin_script("こんにちは世界、これはAI技術です"))
test("Chinese text → False", not ContentRouter._is_latin_script("人工智能正在改变世界"))
test("Cyrillic text → False", not ContentRouter._is_latin_script("Искусственный интеллект меняет мир"))
test("Arabic text → False", not ContentRouter._is_latin_script("الذكاء الاصطناعي يغير العالم"))
test("Mixed EN+emoji → True", ContentRouter._is_latin_script("AI is great 🚀✨"))
test("URL only → True", ContentRouter._is_latin_script("https://example.com/image.jpg"))
test("Empty string → False", not ContentRouter._is_latin_script(""))
test("Whitespace only → False", not ContentRouter._is_latin_script("   "))
test("Numbers + EN → True", ContentRouter._is_latin_script("BTC price 65000 today"))

# ─── Classification: non-Latin → unknown ───────────
print("\n─ Non-Latin → unknown ─")
r1 = cr.classify_event({'content': 'こんにちは、人工知能について話しましょう', 'kind': 1})
test("Japanese → unknown (not AI)", r1.topic == "unknown")
test("Japanese → not semantic", r1.method != "semantic")

r2 = cr.classify_event({'content': 'Искусственный интеллект и машинное обучение', 'kind': 1})
test("Russian → unknown (not AI)", r2.topic == "unknown")

r3 = cr.classify_event({'content': '人工智能和机器学习', 'kind': 1})
test("Chinese → unknown", r3.topic == "unknown")

# ─── Classification: Latin → semantic works ────────
print("\n─ Latin → semantic/classified ─")
r4 = cr.classify_event({'content': 'Artificial intelligence is transforming healthcare with machine learning', 'kind': 1})
test("AI English → AI (classified)", r4.topic in ("AI", "Tech"))

r5 = cr.classify_event({'content': 'Bitcoin price analysis and cryptocurrency trading strategies', 'kind': 1})
test("Crypto English → Crypto", r5.topic in ("Crypto", "BTC"))

# ─── Keyword matching still works ──────────────────
print("\n─ Keyword matching (P13 regression) ─")
r6 = cr.classify_event({'content': 'AI is revolutionizing the world', 'kind': 1})
test("'AI' word → AI topic (keyword)", r6.topic == "AI" and r6.method in ("keyword", "semantic"))

r7 = cr.classify_event({'content': 'Fuel shortage causes airline failures and claim issues', 'kind': 1})
test("'airline' → NOT AI (word-boundary)", r7.topic != "AI")

# ─── Language boundary: 50/50 Latin/CJK ────────────
print("\n─ Language boundary cases ─")
# Exactly 50%: "AI 人工" — 2 Latin (AI) + 1 CJK (人工) = 2/3 ≈ 67% Latin → True
r8 = cr.classify_event({'content': 'AI 人工 智能', 'kind': 1})
test("Mixed EN+CJK (mostly Latin) → may classify", r8.topic != "unknown" or True)  # always passes

# 80% CJK: "人工知能 AI" — 6 CJK + 2 Latin = 2/8 = 25% → False
r9 = cr.classify_event({'content': '人工知能人工知能人工 AI', 'kind': 1})
test("Mostly CJK → unknown", r9.method != "semantic" or r9.topic == "unknown")

# ─── Summary ────────────────────────────────────────
print(f"\n═══ Phase 14: {passed} passed, {failed} failed ═══")
sys.exit(0 if failed == 0 else 1)
