"""
Configuration of quality level scorer.
"""

import pandas as pd

# Weight of the user level in the final score. 
# This value determines how much the user's hierarchy (level) influences the composite score, along with the quality score.
LS_WEIGHT = 0.2

# Quality Level Scorer Configuration DataFrame
QLS_CONFIG_DF = pd.DataFrame({
    "id": [1, 2, 3, 4, 5, 6, 7, 8],
    "tag": [
        "developer",
        "landlord",
        "broker_next",
        "broker_ext_exclusive",
        "broker_ext_titanium",
        "broker_ext_platinum",
        "broker_ext_gold",
        "other_brokers"
    ],
    "description": [
        "User with developer role",
        "User with landlord role",
        "User with broker role and nextagents.mx domain",
        "User with external broker role with some exclusive spot",
        "User with external broker role with Titanium level",
        "User with external broker role with Platinum level",
        "User with external broker role with Gold level",
        "Other users with broker role"
    ],
    "weight": [50, 50, 40, 40, 30, 20, 10, 0]
})
