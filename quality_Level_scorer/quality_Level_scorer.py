import pandas as pd
from quality_level_scorer_config import QLS_CONFIG_DF, LS_WEIGHT


def get_labels_from_ids(user_industria_role_id, user_broker_next_id, user_affiliation_id,
                        spot_exclusive_id, user_level_id):
    """Returns human-readable labels from numeric codes."""
    user_industria_role_map = {
        1: "Tenant", 2: "Broker", 4: "Landlord", 5: "Developer"
    }
    user_level_map = {1: "Gold", 2: "Platinum", 3: "Titanium"}
    user_affiliation_map = {1: "Internal User", 0: "External User"}
    user_broker_next_map = {1: "Yes", 0: "No"}
    spot_exclusive_map = {1: "Yes", 0: "No"}

    return {
        "user_industria_role": user_industria_role_map.get(user_industria_role_id, "Unknown"),
        "user_broker_next": user_broker_next_map.get(user_broker_next_id, "No"),
        "user_affiliation": user_affiliation_map.get(user_affiliation_id, "Unknown"),
        "spot_exclusive": spot_exclusive_map.get(spot_exclusive_id, "No"),
        "user_level": user_level_map.get(user_level_id, "Others")
    }


def level_classifier(user_industria_role_id: int, user_broker_next_id: int,
                        user_affiliation_id: int, spot_exclusive_id: int, user_level_id: int):
    """
    Classifies the spot into a level class ID based on user and broker roles.

    Returns:
    - level_class_id: Integer representing the classification level.
    - log_description: List of textual log entries explaining the decision process.
    """

    log_description = ["➊ Level classification based on user and broker attributes:"]
    level_class_id = 8

    labels = get_labels_from_ids(user_industria_role_id, user_broker_next_id, user_affiliation_id, spot_exclusive_id, user_level_id)
    log_description.append("Reading inputs...")
    log_description.append(f"- User role: {labels['user_industria_role']} (ID {user_industria_role_id})")
    log_description.append(f"- User broker next: {labels['user_broker_next']} (ID {user_broker_next_id})")
    log_description.append(f"- User affiliation: {labels['user_affiliation']} (ID {user_affiliation_id})")
    log_description.append(f"- Exclusive spot: {labels['spot_exclusive']} (ID {spot_exclusive_id})")
    log_description.append(f"- User level: {labels['user_level']} (ID {user_level_id})")

    log_description.append("Evaluating user role...")
    if user_industria_role_id == 5:
        level_class_id = 1
        log_description.append("Developer detected → Level class set to 1.")
    elif user_industria_role_id == 4:
        level_class_id = 2
        log_description.append("Landlord detected → Level class set to 2.")
    elif user_industria_role_id == 2:
        log_description.append("Broker detected. Evaluating broker characteristics...")
        if user_broker_next_id == 1:
            log_description.append("Broker email matches '@nextagents.mx' → Identified as 'Broker Next' → Level class set to 3.")
            level_class_id = 3
        elif user_affiliation_id == 0:
            log_description.append("External broker detected (not internal).")
            if spot_exclusive_id == 1:
                level_class_id = 4
                log_description.append("Exclusive spot found → Level class set to 4.")
            elif user_level_id == 3:
                level_class_id = 5
                log_description.append("User has Titanium level → Level class set to 5.")
            elif user_level_id == 2:
                level_class_id = 6
                log_description.append("User has Platinum level → Level class set to 6.")
            elif user_level_id == 1:
                level_class_id = 7
                log_description.append("User has Gold level → Level class set to 7.")
            else:
                level_class_id = 8
                log_description.append("No special condition met → Level class set to 8 (default classification).")
        else:
            level_class_id = 8
            log_description.append("Internal broker → Level class set to 8 (default classification).")
    else:
        level_class_id = 8
        log_description.append("User role is low-privilege or unrecognized → Level class set to 8 (default classification).")

    # Añadimos descripción del nivel desde el dataframe
    tag_row = QLS_CONFIG_DF[QLS_CONFIG_DF["id"] == level_class_id]
    if not tag_row.empty:
        tag = tag_row["tag"].values[0]
        desc = tag_row["description"].values[0]
        log_description.append(f"Final level class: {level_class_id} ({tag}) — {desc}")
    else:
        log_description.append(f"Final level class: {level_class_id} (Unknown tag)")

    return level_class_id, log_description


def qls_scorer(score: float, level_class_id: int, qls_weight: float, df_config: pd.DataFrame):
    """
    Calculates the Quality Level Score (QLS) using a weighted average.

    Returns:
    - qls_score: Final quality level score.
    - log_description: Log entries explaining the computation.
    """

    log_description = ["➋ Computing the Quality Level Score (QLS):"]

    row = df_config[df_config["id"] == level_class_id]
    if row.empty:
        log_description.append(f"⚠️ No config found for level class {level_class_id}. Default weight = 0.")
        level_weight = 0.0
    else:
        level_weight = row["weight"].values[0]
        log_description.append(f"Level weight from config: {level_weight} (for class {level_class_id})")

    qls_score = (1. - qls_weight) * score + qls_weight * level_weight
    log_description.append(f"QLS = (1 - {qls_weight}) * {score} + {qls_weight} * {level_weight} = {qls_score:.2f}")

    return qls_score, log_description


def qls_with_logs(dic_spot_input: dict):
    """
    Main QLS scoring process with logging of decisions and metadata.
    """

    df_config = QLS_CONFIG_DF.copy()
    qls_weight = LS_WEIGHT
    log_description = []
    debug_info = {}

    # Inputs
    user_industria_role_id = dic_spot_input["user_industria_role_id"]
    user_broker_next_id = dic_spot_input["user_broker_next_id"]
    user_affiliation_id = dic_spot_input["user_affiliation_id"]
    spot_exclusive_id = dic_spot_input["spot_exclusive_id"]
    user_level_id = dic_spot_input["user_level_id"]
    score = dic_spot_input["score"]
    spot_id = dic_spot_input["spot_id"]

    # 1. Level classification
    level_class_id, log_lc = level_classifier(
        user_industria_role_id, user_broker_next_id, user_affiliation_id,
        spot_exclusive_id, user_level_id
    )
    log_description.extend(log_lc)

    # 2. QLS scoring
    qls_score, log_qls = qls_scorer(score, level_class_id, qls_weight, df_config)
    log_description.extend(log_qls)

    # 3. Summary
    log_description.append("➌ Final classification summary:")
    log_description.append(f"✅ Spot {spot_id} has been classified with a QLS score of {qls_score:.2f}.")

    debug_info = {
        "spot_id": spot_id,
        "user_industria_role_id": user_industria_role_id,
        "user_broker_next_id": user_broker_next_id,
        "user_affiliation_id": user_affiliation_id,
        "spot_exclusive_id": spot_exclusive_id,
        "user_level_id": user_level_id,
        "level_class_id": level_class_id,
        "raw_score": score,
        "qls_weight": qls_weight
    }

    return qls_score, {
        "description": log_description,
        "debug": debug_info
    }


def output_qls(list_input: list):
    """
    Applies QLS scoring to a list of input spots, returning scores and detailed logs.
    """
    list_output = []

    for dic_spot_input in list_input:
        qls_score, log = qls_with_logs(dic_spot_input)
        dic_output = {
            "spot_id": dic_spot_input["spot_id"],
            "qls_score": float(qls_score),
            "qls_log": log
        }
        list_output.append(dic_output)

    return list_output

output_qls(list_input)
