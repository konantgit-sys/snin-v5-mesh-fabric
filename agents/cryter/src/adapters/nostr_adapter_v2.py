"""
CRYTER V7.0 — Nostr Adapter (nostr_protocol + websockets)
Publishes signed events to Nostr relay network.
Supports text notes, images, and reply listening.
"""
import json
import time
import logging
import threading
from nostr_sdk import Keys, EventBuilder, Timestamp, Tag, Kind

logger = logging.getLogger(__name__)

# Chart hosting URL
CHART_BASE_URL = "https://upload-v2bot.v2.site/charts/"

class NostrAdapter:
    def __init__(self, nsec=None, relays=None):
        self.relays = [
            "wss://relay.primal.net",
            "wss://nos.lol",
            "wss://relay.damus.io",
            "wss://nostr-pub.wellorder.net",
            "wss://relay.nostr.com.au",
            "wss://offchain.pub",
            "wss://relay.nostr.wirednet.jp",
            "wss://nostr.bitcoiner.social",
            "wss://nostr.oxtr.dev",
            "wss://relay.nostrplebs.com",
            "wss://relay.nostr.net",
            "wss://relay.nostr.info",
            "wss://nostr-verified.wellorder.net",
            "wss://purplepag.es",
            "wss://nostr.sathoarder.com",
            "wss://relay.minibits.cash",
            "wss://nostr.mom",
            "wss://relay.nostrcheck.me",
            "wss://relay.nostr.nu",
            "wss://nostr-pub.semisol.dev",
            "wss://pyramid.fiatjaf.com",
            "wss://nostr.slothy.win",
            "wss://relay.ryzizub.com",
            "wss://nostr.einundzwanzig.space",
            "wss://nostr.vulpem.com",
            "wss://nostr.sovbit.host",
            "wss://relay.nostr.watch",
            "wss://nostr.noones.com",
            "wss://nostrrelay.com",
            "wss://relay.nostr.bg",
            "wss://relay.snort.social",
            "wss://relay.nostr.land",
            "wss://relay.nostr.band",
            "wss://relay.noswhere.com",
            "wss://relay.nostrgraph.net",
            "wss://relay.nostr.wf",
            "wss://relay.nostr.zbd.gg",
            "wss://relay.nostr.kiwi",
            "wss://relay.nostr.li",
            "wss://relay.nostr.ro",
            "wss://relay.nostr.sk",
            "wss://relay.nostr.cz",
            "wss://relay.nostr.pl",
            "wss://relay.nostr.se",
            "wss://relay.nostr.no",
            "wss://relay.nostr.fi",
            "wss://relay.nostr.dk",
            "wss://relay.nostr.be",
            "wss://relay.nostr.at",
            "wss://relay.nostr.ch",
            "ws://127.0.0.1:8198"
        ]
        
        if nsec:
            self.keys = Keys.parse(nsec)
            logger.info(f"NostrAdapter: pubkey={self.keys.public_key().to_bech32()}")
        else:
            self.keys = None
            logger.warning("NostrAdapter: no key — dry run mode")
    
    def publish(self, content, tags=None):
        """Publish text note to all relays."""
        return self._publish_event(content, tags=tags)
    
    def publish_with_chart(self, content, chart_filename, tags=None):
        """Publish text note with chart image URL embedded (Kind 1)."""
        image_url = f"{CHART_BASE_URL}{chart_filename}"
        
        # Add image URL to content — Nostr clients auto-render URLs
        content_with_image = f"{content}\n\n📊 Chart: {image_url}"
        
        # Add imeta tags for client hints (NIP-92)
        all_tags = tags or []
        all_tags.append(["imeta", f"url {image_url}", "m image/jpeg"])
        all_tags.append(["t", "chart"])
        
        # Publish as single text note with image URL
        event_id = self._publish_event(content_with_image, tags=all_tags)
        
        logger.info(f"📊 Chart published: {event_id[:16]}... URL: {image_url}")
        return event_id
    
    def _publish_event(self, content, tags=None, kind=1):
        """Internal: build, sign, and publish event.
        kind: 1=text_note, 3=contact_list, 7=reaction
        """
        if not self.keys:
            logger.info(f"Nostr DRY RUN (kind={kind}): {content[:60]}...")
            return "dry-run-event-id"
        
        nostr_tags = []
        for t in (tags or []):
            try:
                if isinstance(t, list):
                    nostr_tags.append(Tag.parse(t))
                else:
                    nostr_tags.append(t)
            except:
                pass
        
        # Build & sign event based on kind
        if kind == 3:
            # Contact list — content is "" or petnames
            builder = EventBuilder(Kind(kind), content or "")
        elif kind == 7:
            # Reaction — content is "+" or emoji
            builder = EventBuilder(Kind(kind), content or "+")
        else:
            # Default: text note (kind=1)
            builder = EventBuilder.text_note(content)
        
        if nostr_tags:
            builder = builder.tags(nostr_tags)
        builder = builder.custom_created_at(Timestamp.now())
        event = builder.sign_with_keys(self.keys)
        
        event_id = str(event.id())
        event_json = json.loads(event.as_json())
        
        # Publish via WebSocket
        self._publish_websocket(event_json)
        
        logger.info(f"Nostr published (kind={kind}): {event_id[:16]}...")
        return event_id
    
    def _publish_websocket(self, event_json):
        """Send EVENT to all relays via WebSocket."""
        import websocket
        
        def send_to_relay(url, event):
            try:
                ws = websocket.create_connection(url, timeout=10)
                msg = json.dumps(["EVENT", event])
                ws.send(msg)
                result = ws.recv()
                ws.close()
                logger.info(f"  Relay {url}: {result[:80]}")
            except Exception as e:
                logger.warning(f"  Relay {url} failed: {e}")
        
        threads = []
        for relay in self.relays:
            t = threading.Thread(target=send_to_relay, args=(relay, event_json), daemon=True)
            t.start()
            threads.append(t)
        
        # Wait for all (max 15s)
        for t in threads:
            t.join(timeout=15)
    
    def send_dm(self, to_pubkey_hex, content, tags=None):
        """Send encrypted direct message (kind:4).
        to_pubkey_hex: recipient pubkey in hex format
        content: encrypted or plaintext message
        Returns event_id or None on failure.
        """
        if not self.keys:
            logger.warning("No keys — cannot send DM")
            return None
        
        try:
            # Build p-tag for recipient
            all_tags = [Tag.parse(["p", to_pubkey_hex])]
            if tags:
                for t in tags:
                    try:
                        if isinstance(t, list):
                            all_tags.append(Tag.parse(t))
                        else:
                            all_tags.append(t)
                    except:
                        pass
            
            builder = EventBuilder(
                Kind(4),
                content,
            ).tags(all_tags).custom_created_at(Timestamp.now())
            
            event = builder.sign_with_keys(self.keys)
            event_json = json.loads(event.as_json())
            event_id = str(event.id())
            
            self._publish_websocket(event_json)
            logger.info(f"DM sent to {to_pubkey_hex[:16]}... id={event_id[:16]}...")
            return event_id
        except Exception as e:
            logger.error(f"DM send failed: {e}")
            return None
    
    def fetch_recent_events(self, kinds=None, limit=50, since_hours=6):
        """Fetch recent events from relays for challenge scanning.
        Returns list of event dicts.
        """
        import websocket as ws_module
        
        if kinds is None:
            kinds = [1]
        
        events = []
        fetch_relays = [
            "wss://relay.primal.net",
            "wss://nos.lol",
            "wss://relay.damus.io",
        ]
        
        since_ts = int(time.time() - (since_hours * 3600))
        filter_req = json.dumps([
            "REQ", "cryter-fetch",
            {"kinds": kinds, "limit": limit, "since": since_ts}
        ])
        
        for relay_url in fetch_relays:
            try:
                ws = ws_module.create_connection(relay_url, timeout=10)
                ws.send(filter_req)
                ws.settimeout(5)
                
                while True:
                    try:
                        msg = ws.recv()
                        data = json.loads(msg)
                        if isinstance(data, list) and data[0] == "EVENT":
                            event = data[2]
                            events.append({
                                "kind": event.get("kind"),
                                "pubkey": event.get("pubkey", ""),
                                "content": event.get("content", ""),
                                "tags": event.get("tags", []),
                                "id": event.get("id", ""),
                                "created_at": event.get("created_at", 0),
                            })
                        elif isinstance(data, list) and data[0] == "EOSE":
                            break
                    except ws_module.TimeoutError:
                        break
                    except Exception:
                        break
                ws.close()
            except Exception as e:
                logger.debug(f"Fetch from {relay_url}: {e}")
        
        return events
    
    def get_profile(self):
        if not self.keys:
            return {"mode": "dry-run"}
        return {
            "npub": self.keys.public_key().to_bech32(),
            "hex": self.keys.public_key().to_hex(),
            "relays": self.relays
        }
