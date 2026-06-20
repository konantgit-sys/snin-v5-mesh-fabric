#!/usr/bin/env python3
"""
SNIN Knowledge Graph API — serves graph data for visualization.
Reads Redis graph:nodes + graph:edges, returns JSON for vis.js.
"""

import json
import redis
from http.server import HTTPServer, BaseHTTPRequestHandler

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
API_PORT = 8092


def get_graph_data():
    """Fetch all nodes and edges from Redis Knowledge Graph."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    nodes_raw = r.hgetall("graph:nodes")
    edges_raw = r.hgetall("graph:edges")

    nodes = []
    for k, v in nodes_raw.items():
        try:
            node = json.loads(v)
        except json.JSONDecodeError:
            node = {"node_id": k, "raw": v[:200]}
        nodes.append(node)

    edges = []
    for k, v in edges_raw.items():
        try:
            edge = json.loads(v)
        except json.JSONDecodeError:
            edge = {"key": k, "raw": v[:200]}
        edges.append(edge)

    return {"nodes": nodes, "edges": edges, "total_nodes": len(nodes), "total_edges": len(edges)}


class GraphAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/graph":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            data = get_graph_data()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        pass  # suppress logs


if __name__ == "__main__":
    print(f"[Graph API] Starting on port {API_PORT}")
    server = HTTPServer(("0.0.0.0", API_PORT), GraphAPIHandler)
    server.serve_forever()
