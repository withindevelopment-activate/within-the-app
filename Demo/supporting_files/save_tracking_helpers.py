import os, json, ast, logging, pandas as pd, pytz, uuid, re, traceback
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from django.core.cache import cache
import itertools
from Demo.supporting_files.supporting_functions import get_uae_current_date
import requests
from django.contrib import messages

# Import keys
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)


def clean_utm_row(row):
    return {
        "UTM_Source": (row.get("UTM_Source") or "").strip().lower(),
        "UTM_Medium": (row.get("UTM_Medium") or "").strip(),
        "UTM_Campaign": (row.get("UTM_Campaign") or "").strip(),
        "UTM_Term": (row.get("UTM_Term") or "").strip(),
        "UTM_Content": (row.get("UTM_Content") or "").strip()
    }


def utm_richness_score(row):
    return sum([
        bool(row["UTM_Medium"]),
        bool(row["UTM_Campaign"]),
        bool(row["UTM_Term"]),
        bool(row["UTM_Content"])
    ])


def recover_utms(final_source, incoming_utms, all_rows):
    """
    Recover richest UTMs matching final_source from historical rows.
    """

    ## Clean all the rows
    for r in all_rows:
        r["UTM_Source"] = (r.get("UTM_Source") or "").strip().lower()
        r["UTM_Medium"] = (r.get("UTM_Medium") or "").strip()
        r["UTM_Campaign"] = (r.get("UTM_Campaign") or "").strip()
        r["UTM_Term"] = (r.get("UTM_Term") or "").strip()
        r["UTM_Content"] = (r.get("UTM_Content") or "").strip()

    utm_medium, utm_campaign, utm_term, utm_content = incoming_utms

    # If frontend UTMs exist >> never override
    if any([utm_medium, utm_campaign, utm_term, utm_content]):
        return utm_medium, utm_campaign, utm_term, utm_content

    candidates = []

    for row in all_rows:

        row = clean_utm_row(row)

        if row["UTM_Source"] != final_source:
            continue

        score = utm_richness_score(row)

        if score > 0:
            candidates.append((score, row))

    if not candidates:
        return utm_medium, utm_campaign, utm_term, utm_content

    best = sorted(candidates, key=lambda x: -x[0])[0][1]

    return (
        best["UTM_Medium"],
        best["UTM_Campaign"],
        best["UTM_Term"],
        best["UTM_Content"]
    )


def backfill_missing_utms(final_source, utm_medium, utm_campaign, utm_term, utm_content, visitor_id=None, session_id=None, mobile=None, sleec_id=None):
    payload = {
        "UTM_Medium": utm_medium,
        "UTM_Campaign": utm_campaign,
        "UTM_Term": utm_term,
        "UTM_Content": utm_content,
        "Last_Updated": get_uae_current_date()
    }

    try:

        if session_id:

            supabase.table("Tracking_Visitors_duplicate") \
                .update(payload) \
                .eq("Session_ID", session_id) \
                .eq("UTM_Source", final_source) \
                .is_("UTM_Campaign", None) \
                .execute()

        if visitor_id:

            supabase.table("Tracking_Visitors_duplicate") \
                .update(payload) \
                .eq("Visitor_ID", visitor_id) \
                .eq("UTM_Source", final_source) \
                .is_("UTM_Campaign", None) \
                .execute()

        if mobile:

            supabase.table("Tracking_Visitors_duplicate") \
                .update(payload) \
                .eq("Customer_Mobile", mobile) \
                .eq("UTM_Source", final_source) \
                .is_("UTM_Campaign", None) \
                .execute()

        if sleec_id:

            supabase.table("Tracking_Visitors_duplicate") \
                .update(payload) \
                .eq("SleecID", sleec_id) \
                .eq("UTM_Source", final_source) \
                .is_("UTM_Campaign", None) \
                .execute()

    except Exception as e:
        print("[UTM BACKFILL ERROR]", e)

def get_history_rows(session_id=None, visitor_id=None, mobile=None, sleec_id=None):
    ## Initialize an empty list
    ##
    history_rows = []

    ##
    try:
        if session_id:
            res = supabase.table("Tracking_Visitors_duplicate") \
                .select("UTM_Source, UTM_Medium, UTM_Campaign, UTM_Term, UTM_Content") \
                .eq("Session_ID", session_id) \
                .execute()

            history_rows.extend(res.data or [])

        if visitor_id:
            res = supabase.table("Tracking_Visitors_duplicate") \
                .select("UTM_Source, UTM_Medium, UTM_Campaign, UTM_Term, UTM_Content") \
                .eq("Visitor_ID", visitor_id) \
                .execute()

            history_rows.extend(res.data or [])

        if mobile:
            res = supabase.table("Tracking_Visitors_duplicate") \
                .select("UTM_Source, UTM_Medium, UTM_Campaign, UTM_Term, UTM_Content") \
                .eq("Customer_Mobile", mobile) \
                .execute()

            history_rows.extend(res.data or [])

        if sleec_id:
            res = supabase.table("Tracking_Visitors_duplicate") \
                .select("UTM_Source, UTM_Medium, UTM_Campaign, UTM_Term, UTM_Content") \
                .eq("SleecID", sleec_id) \
                .execute()

            history_rows.extend(res.data or [])


        return history_rows
    
    except Exception as e:
        print(f"[UTM HISTORY FETCH ERROR] {e}")
        return []



