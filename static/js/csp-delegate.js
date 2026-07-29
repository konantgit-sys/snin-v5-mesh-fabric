/**
 * CSP Event Delegate — replaces inline event handlers (onclick/onchange/oninput/onkeydown).
 * Uses data-action attributes instead of inline handlers for nonce-based CSP.
 * 
 * Format: data-action="funcName" | "funcName:arg1,arg2" | "call1|call2:arg"
 */

(function() {
    'use strict';

    /**
     * Execute a data-action string.
     * Supports:
     *   "funcName"              → window.funcName()
     *   "funcName:arg"          → window.funcName('arg')
     *   "funcName:arg1,arg2"    → window.funcName('arg1', 'arg2')
     *   "Obj.method:arg"        → window.Obj.method('arg')
     *   "func1|func2:arg"       → sequential calls
     *   "this.method"           → element.method()
     */
    function executeAction(action, element, event) {
        if (!action) return;
        
        const calls = action.split('|');
        for (const call of calls) {
            const trimmed = call.trim();
            if (!trimmed) continue;
            
            // Handle 'this.method' pattern
            if (trimmed.startsWith('this.')) {
                const method = trimmed.substring(5);
                if (typeof element[method] === 'function') {
                    element[method]();
                }
                continue;
            }
            
            // Handle document.getElementById patterns  
            if (trimmed.startsWith('document.')) {
                try {
                    // Use Function only for our own HTML code — safe since not user-provided
                    const fn = new Function('event', 'element', trimmed);
                    fn(event, element);
                } catch(e) {
                    console.warn('data-action error:', trimmed, e);
                }
                continue;
            }
            
            // Handle I18N.setLang and other nested object patterns
            if (trimmed.includes('.') && !trimmed.startsWith('this.')) {
                const colonIdx = trimmed.indexOf(':');
                const funcPath = colonIdx > -1 ? trimmed.substring(0, colonIdx) : trimmed;
                const argStr = colonIdx > -1 ? trimmed.substring(colonIdx + 1) : '';
                
                const parts = funcPath.split('.');
                let obj = window;
                for (const part of parts) {
                    if (obj && typeof obj === 'object') {
                        obj = obj[part];
                    } else {
                        obj = undefined;
                        break;
                    }
                }
                
                if (typeof obj === 'function') {
                    const args = argStr ? argStr.split(',').map(s => s.trim()) : [];
                    obj.apply(window, args);
                    continue;
                }
            }
            
            // Standard: funcName or funcName:arg1,arg2
            const colonIdx = trimmed.indexOf(':');
            const funcName = colonIdx > -1 ? trimmed.substring(0, colonIdx) : trimmed;
            const argStr = colonIdx > -1 ? trimmed.substring(colonIdx + 1) : '';
            
            const fn = window[funcName];
            if (typeof fn === 'function') {
                if (argStr) {
                    const args = argStr.split(',').map(s => s.trim());
                    fn.apply(window, args);
                } else {
                    fn();
                }
            } else {
                console.warn('data-action: function not found:', funcName);
            }
        }
    }

    // Click delegation (replaces onclick)
    document.addEventListener('click', function(e) {
        const el = e.target.closest('[data-action]');
        if (el) {
            e.preventDefault();
            const action = el.getAttribute('data-action');
            executeAction(action, el, e);
        }
    }, true);

    // Change delegation (replaces onchange)
    document.addEventListener('change', function(e) {
        const el = e.target.closest('[data-action]');
        if (el && el.tagName === 'SELECT') {
            const action = el.getAttribute('data-action');
            executeAction(action, el, e);
        }
    }, true);

    // Input delegation (replaces oninput)
    document.addEventListener('input', function(e) {
        const el = e.target.closest('[data-action]');
        if (el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')) {
            const action = el.getAttribute('data-action');
            executeAction(action, el, e);
        }
    }, true);

    // Keydown handlers (replaces onkeydown — moved from HTML)
    // Search inputs: Enter triggers search
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            // Search inputs (id=searchInput, searchInputTab)
            if (e.target.id === 'searchInput' || e.target.id === 'searchInputTab') {
                e.preventDefault();
                if (typeof doSearch === 'function') doSearch();
            }
            // DM textarea (id=dmInput) — Enter without Shift sends
            if (e.target.id === 'dmInput' && !e.shiftKey) {
                e.preventDefault();
                if (typeof sendDM === 'function') sendDM();
            }
        }
    }, true);

    console.log('[CSP] Event delegation active — nonce-based CSP, no inline handlers');
})();
