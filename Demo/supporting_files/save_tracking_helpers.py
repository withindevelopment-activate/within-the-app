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

    print("\n==============================")
    print("[RECOVER UTMS START]")
    print("Final Source:", final_source)
    print("Incoming UTMs:", incoming_utms)
    print("Total historical rows:", len(all_rows))
    print("==============================")

    # -----------------------------
    # Clean all rows
    # -----------------------------
    print("\n[CLEANING HISTORICAL ROWS]")

    for i, r in enumerate(all_rows):


        r["UTM_Source"] = (r.get("UTM_Source") or "").strip().lower()
        r["UTM_Medium"] = (r.get("UTM_Medium") or "").strip()
        r["UTM_Campaign"] = (r.get("UTM_Campaign") or "").strip()
        r["UTM_Term"] = (r.get("UTM_Term") or "").strip()
        r["UTM_Content"] = (r.get("UTM_Content") or "").strip()


    utm_medium, utm_campaign, utm_term, utm_content = incoming_utms

    print("\n[FRONTEND UTM CHECK]")
    print("utm_medium:", utm_medium)
    print("utm_campaign:", utm_campaign)
    print("utm_term:", utm_term)
    print("utm_content:", utm_content)

    # -----------------------------
    # If frontend UTMs exist
    # -----------------------------
    if any([utm_medium, utm_campaign, utm_term, utm_content]):
        print("\n[FRONTEND UTMS PRESENT — SKIPPING RECOVERY]")
        return utm_medium, utm_campaign, utm_term, utm_content

    print("\n[NO FRONTEND UTMS — STARTING RECOVERY]")

    candidates = []

    # -----------------------------
    # Scan historical rows
    # -----------------------------
    for i, row in enumerate(all_rows):

        print("\n----------------------------")
        print(f"[ROW {i}] RAW:", row)

        row = clean_utm_row(row)

        print(f"[ROW {i}] CLEANED:", row)

        if row["UTM_Source"] != final_source:
            print(
                f"[ROW {i}] SOURCE MISMATCH:",
                row["UTM_Source"], "!=" , final_source
            )
            continue

        print(f"[ROW {i}] SOURCE MATCH")

        score = utm_richness_score(row)

        print(f"[ROW {i}] RICHNESS SCORE:", score)

        if score > 0:
            print(f"[ROW {i}] ADDED AS CANDIDATE")
            candidates.append((score, row))
        else:
            print(f"[ROW {i}] SCORE TOO LOW — SKIPPED")

    # -----------------------------
    # No candidates
    # -----------------------------
    if not candidates:
        print("\n[NO VALID CANDIDATES FOUND]")
        print("Returning original incoming UTMs")
        return utm_medium, utm_campaign, utm_term, utm_content

    # -----------------------------
    # Choose best candidate
    # -----------------------------
    print("\n[CANDIDATES FOUND]")
    for score, row in candidates:
        print("Score:", score, "| Row:", row)

    best = sorted(candidates, key=lambda x: -x[0])[0][1]

    print("\n[BEST CANDIDATE SELECTED]")
    print(best)

    result = (
        best["UTM_Medium"],
        best["UTM_Campaign"],
        best["UTM_Term"],
        best["UTM_Content"]
    )

    print("\n[FINAL RECOVERED UTMS]")
    print(result)

    print("\n[RECOVER UTMS END]")
    print("==============================\n")

    return result


'''def backfill_missing_utms(final_source, utm_medium, utm_campaign, utm_term, utm_content, visitor_id=None, session_id=None, mobile=None, sleec_id=None):
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
        print("[UTM BACKFILL ERROR]", e)'''

'''def get_history_rows(session_id=None, visitor_id=None, mobile=None, sleec_id=None):
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
        return []'''


def get_history_rows(session_id=None, visitor_id=None, mobile=None, sleec_id=None):
    """
    Fetch historical tracking rows from Tracking_Visitors_duplicate.
    Queries each filter separately to fully use indexes, then merges results.
    """
    filters_map = {
        "Session_ID": session_id,
        "Visitor_ID": visitor_id,
        "Customer_Mobile": mobile,
        "SleecID": sleec_id
    }

    results = []

    # Query each filter separately to leverage indexes
    for col, val in filters_map.items():
        if val:
            try:
                res = supabase.table("Tracking_Visitors_duplicate") \
                    .select(
                        "Distinct_ID, Session_ID, Visitor_ID, Customer_Mobile, SleecID, "
                        "UTM_Source, UTM_Medium, UTM_Campaign, UTM_Term, UTM_Content, "
                        "Timezone, Screen_Resolution"
                    ) \
                    .eq(col, val) \
                    .execute()
                
                if res.data:
                    results.extend(res.data)
            except Exception as e:
                print(f"[UTM HISTORY FETCH ERROR] {col}", e)

    return results


def backfill_missing_utms(
    final_source,
    utm_medium,
    utm_campaign,
    utm_term,
    utm_content,
    visitor_id=None,
    session_id=None,
    mobile=None,
    sleec_id=None
):

    if not any([utm_medium, utm_campaign, utm_term, utm_content]):
        return

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



####################################
##################### Helpers in the Main Save Tracking function

