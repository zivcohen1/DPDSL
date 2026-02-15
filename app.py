import os
import io
import sys
import uuid
import sqlite3
from contextlib import redirect_stdout
from flask import Flask, render_template, request, jsonify, session

# Import both rewriters
from rewriter import create_rewriter, DPDSLRewriter
from dp_rewriter import rewrite_and_execute, BudgetManager

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24).hex())

# Use absolute path to database file (works in any working directory)
DB_PATH = os.path.join(os.path.dirname(__file__), "employee_faker.db")

# --- Database connection helper ---
def get_db():
    """Get a sqlite3 connection to the faker database."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

# --- Production rewriter (singleton per process) ---
_prod_rewriter = None

def get_prod_rewriter() -> DPDSLRewriter:
    global _prod_rewriter
    if _prod_rewriter is None:
        _prod_rewriter = create_rewriter(database_path=DB_PATH)
    return _prod_rewriter

# --- Dev mode budget managers (keyed by session user_id) ---
_dev_budgets: dict[str, BudgetManager] = {}

def get_dev_budget(user_id: str) -> BudgetManager:
    if user_id not in _dev_budgets:
        _dev_budgets[user_id] = BudgetManager(max_budget=10.0)
    return _dev_budgets[user_id]

# --- Ensure session has a user_id ---
def ensure_user_id():
    if "user_id" not in session:
        session["user_id"] = uuid.uuid4().hex[:12]
    return session["user_id"]


# ========== ROUTES ==========

@app.route("/")
def index():
    ensure_user_id()
    return render_template("index.html")


@app.route("/query", methods=["POST"])
def run_query():
    user_id = ensure_user_id()
    data = request.get_json(force=True)
    query_text = data.get("query", "").strip()
    mode = data.get("mode", "production")

    if not query_text:
        return jsonify({"error": "Query cannot be empty."}), 400

    if mode == "production":
        return _run_production(query_text, user_id)
    else:
        return _run_development(query_text, user_id)


@app.route("/budget")
def budget_status():
    user_id = ensure_user_id()
    mode = request.args.get("mode", "production")

    if mode == "production":
        rewriter = get_prod_rewriter()
        info = rewriter.get_user_budget_status(user_id)
    else:
        bm = get_dev_budget(user_id)
        info = {
            "remaining": bm.remaining(),
            "spent": bm.current_spent,
            "max": bm.max_budget,
            "queries": len(bm.query_log),
        }

    return jsonify(info)


@app.route("/reset", methods=["POST"])
def reset_budget():
    user_id = ensure_user_id()
    mode = request.get_json(force=True).get("mode", "production")

    if mode == "production":
        get_prod_rewriter().reset_user_budget(user_id)
    else:
        if user_id in _dev_budgets:
            _dev_budgets[user_id].reset()

    return jsonify({"ok": True})


# ========== INTERNAL HELPERS ==========

def _run_production(query_text: str, user_id: str):
    rewriter = get_prod_rewriter()
    result, error = rewriter.execute_query(query_text, user_id, verbose=False)
    budget = rewriter.get_user_budget_status(user_id)

    if error:
        return jsonify({"error": error, "budget": budget})

    return jsonify({
        "results": [list(row) for row in result] if result else [],
        "budget": budget,
    })


def _run_development(query_text: str, user_id: str):
    bm = get_dev_budget(user_id)

    # Capture verbose print output
    capture = io.StringIO()
    conn = get_db()
    try:
        with redirect_stdout(capture):
            result, errors = rewrite_and_execute(
                query_text, conn, verbose=True, budget_manager=bm
            )
    finally:
        conn.close()

    verbose_log = capture.getvalue()

    budget = {
        "remaining": bm.remaining(),
        "spent": bm.current_spent,
        "max": bm.max_budget,
        "queries": len(bm.query_log),
    }

    if errors:
        error_msg = errors if isinstance(errors, str) else "\n".join(errors)
        return jsonify({"error": error_msg, "verbose_log": verbose_log, "budget": budget})

    return jsonify({
        "results": [list(row) for row in result] if result else [],
        "verbose_log": verbose_log,
        "budget": budget,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
