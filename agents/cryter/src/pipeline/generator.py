"""
CRYTER V10 — PIPELINE: GENERATOR
Step 3: Generate EN + RU content via LLM.
V10 Phase 1: Memory Spine — inject vector memory context before generation.
V10 Phase 2: Diversity Guard — detect & avoid semantic duplicates.
V10 Phase 6: Language Validation — detect & fix wrong language in nostr field.
"""
import time
import logging

logger = logging.getLogger("pipeline.generator")

# Maximum regeneration attempts per post
MAX_REGENERATION_ATTEMPTS = 2
# Maximum language fix attempts
MAX_LANGUAGE_FIX_ATTEMPTS = 2


def _load_config():
    """Load V10 feature flags from config."""
    import yaml
    try:
        with open("config/config.yaml") as f:
            config = yaml.safe_load(f) or {}
        return config.get("v10_features", {})
    except Exception:
        return {}


def _build_draft_text(snapshot):
    """Build a short draft text from market snapshot for similarity search."""
    prices = snapshot.get('prices', {})
    funding = snapshot.get('funding', {})
    parts = []
    if prices.get('btc'):
        parts.append(f"BTC=${prices['btc']}")
    if prices.get('eth'):
        parts.append(f"ETH=${prices['eth']}")
    if funding.get('funding_rate'):
        parts.append(f"funding={funding['funding_rate']*100:.4f}%")
    fg = snapshot.get('fear_greed')
    if fg:
        parts.append(f"Fear&Greed={fg}")
    return " ".join(parts) if parts else "market analysis"


def _get_or_create_vm(v10_config):
    """Get or create VectorMemory instance from config."""
    memory_config = v10_config.get("memory_spine", {})
    if not memory_config.get("enabled", False):
        return None
    try:
        from core.vector_memory import VectorMemory
        db_path = memory_config.get("db_path", "data/cryter_v7.db")
        return VectorMemory(db_path)
    except Exception as e:
        logger.warning(f"VectorMemory init failed: {e}")
        return None


def _check_diversity(text, vm, v10_config):
    """Check if generated text is too similar to past posts."""
    guard_config = v10_config.get("diversity_guard", {})
    if not guard_config.get("enabled", False):
        return {"is_duplicate": False, "similar_posts": [], "max_similarity": 0.0, "duplicate_ids": []}
    
    if not vm:
        return {"is_duplicate": False, "similar_posts": [], "max_similarity": 0.0, "duplicate_ids": []}
    
    try:
        from core.content_guard import ContentGuard
        threshold = guard_config.get("threshold", 0.75)
        guard = ContentGuard(vm, threshold=threshold)
        result = guard.check(text)
        
        # V10.2: Record duplicate rate
        try:
            from core.metrics_collector import metrics
            metrics.record_post(result.get("is_duplicate", False))
        except Exception:
            pass
        
        return result
    except Exception as e:
        logger.warning(f"ContentGuard check failed: {e}")
        return {"is_duplicate": False, "similar_posts": [], "max_similarity": 0.0, "duplicate_ids": []}


def generate_content(snapshot, history_context, prompt_engine, llm, cycle_id, strategist_feedback=""):
    """
    Generate bilingual posts via DegradationEngine.
    V10 Phase 1: Injects vector memory context for narrative continuity.
    V10 Phase 2: Checks for semantic duplicates and regenerates if needed.
    V10 Phase 3: Engages feedback loop — what worked gets amplified.
    V10 Phase 3: Strategist DM feedback injected into prompt.
    
    Returns:
        dict with 'en', 'ru', 'nostr_en', 'nostr_ru' texts
    """
    logger.info("✍️  GENERATING content (EN + RU)...")
    
    # ── Phase 0: Content Grid Injection ──
    try:
        from core.content_grid import get_schedule_instruction, get_today_schedule
        grid_schedule = get_today_schedule()
        grid_instruction = get_schedule_instruction()
        if grid_schedule:
            history_context['content_grid'] = {
                'category': grid_schedule['category'],
                'tag': grid_schedule['tag'],
                'instruction': grid_instruction,
            }
            # Inject into memory context so prompt_engine sees it
            grid_block = f"[CONTENT GRID] {grid_instruction}"
            if history_context.get('memory_context'):
                history_context['memory_context'] = grid_block + "\n\n" + history_context['memory_context']
            else:
                history_context['memory_context'] = grid_block
            logger.info(f"📅 Content Grid: {grid_schedule['category']} day — #{grid_schedule['tag']}")
    except Exception as e:
        logger.debug(f"Content Grid: {e}")
    
    v10 = _load_config()
    vm = _get_or_create_vm(v10)
    memory_config = v10.get("memory_spine", {})
    feedback_config = v10.get("engagement_feedback", {})
    
    # ── Phase 1: Memory Context Injection ──
    memory_context = ""
    if vm:
        try:
            draft = _build_draft_text(snapshot)
            max_similar = memory_config.get("max_similar", 3)
            memory_context = vm.get_context_block(draft, max_similar=max_similar)
            if memory_context:
                logger.info(f"🧠 Phase 1: Memory Spine — injected {len(memory_context)} chars")
            else:
                logger.info("🧠 Phase 1: Memory Spine — no similar posts found (first run)")
        except Exception as e:
            logger.warning(f"Memory Spine error: {e}")
    
    # Inject into history_context so prompt_engine picks it up
    history_context['memory_context'] = memory_context
    
    # ── V10 Phase 3: Strategist DM Feedback ──
    if strategist_feedback:
        if history_context.get('memory_context'):
            history_context['memory_context'] += "\n\n" + strategist_feedback
        else:
            history_context['memory_context'] = strategist_feedback
        logger.info(f"📬 Strategist feedback injected ({len(strategist_feedback)} chars)")
    
    # ── Phase 3: Engagement Feedback ──
    if feedback_config.get("enabled", False):
        try:
            from core.engagement_bridge import EngagementBridge
            db_path = memory_config.get("db_path", "data/cryter_v7.db")
            bridge = EngagementBridge(db_path)
            
            # Refresh scores for recent posts
            window_hours = feedback_config.get("window_hours", 2.5)
            updated = bridge.update_latest(hours_back=window_hours)
            
            # Get feedback context
            feedback_context = bridge.build_feedback_context()
            
            # Inject into history_context
            history_context['feedback_context'] = feedback_context
            history_context['memory_context'] = (memory_context + "\n\n" + feedback_context 
                                                  if memory_context else feedback_context)
            
            logger.info(f"📊 Phase 3: Engagement Feedback — {updated} posts refreshed, "
                       f"feedback_context={len(feedback_context)} chars")
        except Exception as e:
            logger.warning(f"Phase 3: Engagement Feedback error: {e}")

    # ── Phase 4 (BRING): Narrative arc injection ──
    narrative_context = ""
    try:
        import sys, os
        bring_path = os.path.join(os.path.dirname(__file__), "..", "..", "bring_modules")
        sys.path.insert(0, os.path.abspath(bring_path))
        from bring_narrative import get_narrative_engine, enhance_prompt_with_narrative
        db_path = memory_config.get("db_path", "data/cryter_v7.db")
        narrative_eng = get_narrative_engine(db_path)
        narrative_context = enhance_prompt_with_narrative(
            base_prompt="",
            narrative_engine=narrative_eng,
            mood=history_context.get('mood', 'NEUTRAL'),
        )
        if narrative_context and "NARRATIVE ARC CONTEXT" in narrative_context:
            history_context['narrative_context'] = narrative_context
            if history_context.get('memory_context'):
                history_context['memory_context'] += "\n\n" + narrative_context
            else:
                history_context['memory_context'] = narrative_context
            logger.info(f"📖 Phase 4 (BRING): Narrative context injected ({len(narrative_context)} chars)")
    except Exception as e:
        logger.debug(f"Phase 4 (BRING): Narrative engine not available ({e})")
    
    # ── Phase 5 (BRING): Audience engagement context ──
    try:
        bring_path = os.path.join(os.path.dirname(__file__), "..", "..", "bring_modules")
        sys.path.insert(0, os.path.abspath(bring_path))
        from bring_engagement import get_engagement_engine, build_engagement_prompt_context
        db_path = memory_config.get("db_path", "data/cryter_v7.db")
        eng = get_engagement_engine(db_path)
        engagement_ctx = build_engagement_prompt_context(eng, "nostr")
        if engagement_ctx:
            if history_context.get('memory_context'):
                history_context['memory_context'] += "\n\n[AUDIENCE]\n" + engagement_ctx
            else:
                history_context['memory_context'] = "[AUDIENCE]\n" + engagement_ctx
            logger.info(f"👥 Phase 5 (BRING): Engagement context injected ({len(engagement_ctx)} chars)")
    except Exception as e:
        logger.debug(f"Phase 5 (BRING): Engagement engine not available ({e})")

    # ── V10 Phase 3: Adaptive System Prompt ──
    try:
        from core.adaptive_system_prompt import AdaptivePromptBuilder
        adaptive = AdaptivePromptBuilder(db_path)
        base_prompt = "Generate a thoughtful crypto analysis post."
        adaptive_instructions = adaptive.build_adaptive_prompt(base_prompt)
        # Inject the adaptive context AFTER base prompt (instructions part only)
        if "--- ADAPTIVE CONTEXT ---" in adaptive_instructions:
            context_section = adaptive_instructions.split("--- ADAPTIVE CONTEXT ---")[-1].strip()
            if context_section:
                memory_ctx = history_context.get('memory_context', '')
                history_context['memory_context'] = (memory_ctx + "\n\n" + context_section 
                                                      if memory_ctx else context_section)
                history_context['adaptive_instructions'] = context_section
                logger.info(f"🧬 Phase 3: Adaptive Prompt — injected {len(context_section)} chars")
    except Exception as e:
        logger.warning(f"Phase 3: Adaptive Prompt error: {e}")
    
    # ── Phase 2: Diversity Guard ──
    guard_config = v10.get("diversity_guard", {})
    guard_enabled = guard_config.get("enabled", False)
    max_retries = guard_config.get("max_retries", MAX_REGENERATION_ATTEMPTS)
    
    # Generate with regeneration loop
    for attempt in range(max_retries + 1):
        # EN Generation
        logger.info("🇬🇧 Generating EN post...")
        prompt_en = prompt_engine.build_prompt(snapshot, history_context, language='en')
        raw_en = llm.generate(prompt_en, max_tokens=4000, language='en', cycle_id=cycle_id)
        parsed_en, ok_en = llm.parse_dual_stream(raw_en)
        tg_text_en = parsed_en['telegram']
        nostr_text_en = parsed_en.get('nostr', '')
        logger.info(f"✅ EN: {len(tg_text_en)} chars (Telegram), {len(nostr_text_en)} chars (Nostr)")
        
        # RU Generation
        logger.info("🇷🇺 Generating RU post...")
        time.sleep(5)
        prompt_ru = prompt_engine.build_prompt(snapshot, history_context, language='ru')
        raw_ru = llm.generate(prompt_ru, max_tokens=4000, language='ru', cycle_id=cycle_id)
        parsed_ru, ok_ru = llm.parse_dual_stream(raw_ru)
        tg_text_ru = parsed_ru['telegram']
        nostr_text_ru = parsed_ru.get('nostr', '')
        logger.info(f"✅ RU: {len(tg_text_ru)} chars (Telegram), {len(nostr_text_ru)} chars (Nostr)")
        
        # ── V10 Phase 6: Language Validation ──
        ru_lang_ok = False
        for lang_attempt in range(MAX_LANGUAGE_FIX_ATTEMPTS + 1):
            from pipeline.validation import validate_single, build_repair_prompt
            ru_nostr_check = validate_single(nostr_text_ru, 'ru', 'RU_nostr')
            
            if ru_nostr_check['passed']:
                ru_lang_ok = True
                if lang_attempt > 0:
                    logger.info(f"🔁 Language fix #{lang_attempt}: nostr_ru теперь на русском ✅")
                break
            
            # Определяем какая ошибка
            issues_str = '; '.join(ru_nostr_check['issues'])
            logger.warning(f"🔁 Language fix #{lang_attempt}: nostr_ru — {issues_str}")
            
            if lang_attempt < MAX_LANGUAGE_FIX_ATTEMPTS:
                # Строим repair prompt
                val_result = {
                    'all_passed': False,
                    'issues': [f"RU_nostr: {issues_str}"],
                    'en': {'checks': {}},
                    'ru': {'checks': {'nostr': ru_nostr_check}}
                }
                repair = build_repair_prompt(val_result, prompt_ru)
                time.sleep(3)
                raw_ru = llm.generate(repair, max_tokens=4000, language='ru', cycle_id=cycle_id)
                parsed_ru, ok_ru = llm.parse_dual_stream(raw_ru)
                tg_text_ru = parsed_ru['telegram']
                nostr_text_ru = parsed_ru.get('nostr', '')
                logger.info(f"🔄 Regen RU (lang fix #{lang_attempt+1}): "
                           f"tg={len(tg_text_ru)} chars, nt={len(nostr_text_ru)} chars")
            else:
                logger.warning(f"⚠️ Language fix exhausted: publishing nostr_ru as-is ({issues_str})")
        
        # Логируем итог валидации
        from pipeline.validation import validate_single as vs
        en_tg_check = vs(tg_text_en, 'en', 'EN_telegram')
        en_nt_check = vs(nostr_text_en, 'en', 'EN_nostr')
        ru_tg_check = vs(tg_text_ru, 'ru', 'RU_telegram')
        ru_nt_check = vs(nostr_text_ru, 'ru', 'RU_nostr')
        
        all_langs_ok = all(c['passed'] for c in [en_tg_check, en_nt_check, ru_tg_check, ru_nt_check])
        if not all_langs_ok:
            fail_list = [c['label'] for c in [en_tg_check, en_nt_check, ru_tg_check, ru_nt_check] if not c['passed']]
            logger.warning(f"⚠️ Language issues remain: {', '.join(fail_list)}")
        else:
            logger.info(f"✅ Language validation: EN(tg={en_tg_check['length']} nt={en_nt_check['length']}) "
                       f"RU(tg={ru_tg_check['length']} nt={ru_nt_check['length']})")
        
        # ── Diversity check ──
        if not guard_enabled:
            break  # skip check, use generated content
        
        # Check EN Telegram text
        en_check = _check_diversity(tg_text_en, vm, v10)
        ru_check = _check_diversity(tg_text_ru, vm, v10)
        
        is_dup = en_check.get('is_duplicate') or ru_check.get('is_duplicate')
        
        if not is_dup:
            logger.info(f"🛡️ Phase 2: content unique (max sim={en_check.get('max_similarity', 0):.3f})")
            break
        
        # Duplicate detected — prepare for regeneration
        from core.content_guard import ContentGuard
        guard = ContentGuard(vm, threshold=guard_config.get("threshold", 0.75))
        
        if attempt < max_retries:
            dup_ids = en_check.get('duplicate_ids', []) + ru_check.get('duplicate_ids', [])
            logger.warning(
                f"🛡️ Phase 2: DUPLICATE detected (attempt {attempt+1}/{max_retries+1}, "
                f"max_sim={en_check.get('max_similarity', 0):.3f}, dup_ids={dup_ids})"
            )
            # Build avoidance instruction
            all_similar = en_check.get('similar_posts', []) + ru_check.get('similar_posts', [])
            avoidance = guard.get_avoidance_instruction(all_similar)
            # Inject avoidance into memory_context for next attempt
            history_context['memory_context'] = memory_context + "\n\n" + avoidance if memory_context else avoidance
            logger.info(f"🔁 Regenerating with avoidance instruction ({len(avoidance)} chars)...")
        else:
            logger.warning(f"🛡️ Phase 2: max retries ({max_retries}) reached. Publishing as-is.")
            break
    
    # Fact-check
    try:
        from core.fact_checker import FactChecker
        fc = FactChecker()
        for label, text in [('EN', tg_text_en), ('RU', tg_text_ru)]:
            ok, issues = fc.validate(text, snapshot)
            if not ok:
                logger.warning(f"⚠️ FactCheck {label}: {fc.format_issues()}")
            else:
                logger.info(f"✅ FactCheck {label}: OK")
    except Exception as e:
        logger.warning(f"FactCheck unavailable: {e}")
    
    return {
        'en': tg_text_en,
        'ru': tg_text_ru,
        'nostr_en': nostr_text_en,
        'nostr_ru': nostr_text_ru
    }
