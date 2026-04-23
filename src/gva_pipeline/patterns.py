from __future__ import annotations

import re
from typing import Final


TEXT_WHITESPACE_RE: Final[re.Pattern[str]] = re.compile(r"\s+")
SENTENCE_SPLIT_RE: Final[re.Pattern[str]] = re.compile(r"(?<=[.!?])\s+")

EXPLICIT_SCHOOL_LOCATION_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\bon campus\b", re.IGNORECASE),
    re.compile(r"\bschool grounds\b", re.IGNORECASE),
    re.compile(r"\binside (?:a|the) school\b", re.IGNORECASE),
    re.compile(r"\bat (?:a|the) school\b", re.IGNORECASE),
    re.compile(r"\bin(?:side)? (?:a|the) classroom\b", re.IGNORECASE),
    re.compile(r"\bin(?:side)? (?:a|the) dorm(?:itory)?\b", re.IGNORECASE),
)

CONTEXT_PATTERNS: Final[dict[str, tuple[re.Pattern[str], ...]]] = {
    "mentions_party": (
        re.compile(r"\bparty\b", re.IGNORECASE),
        re.compile(r"\bhouse party\b", re.IGNORECASE),
        re.compile(r"\bpool party\b", re.IGNORECASE),
        re.compile(r"\bbirthday party\b", re.IGNORECASE),
        re.compile(r"\bgraduation party\b", re.IGNORECASE),
        re.compile(r"\bblock party\b", re.IGNORECASE),
        re.compile(r"\bcookout\b", re.IGNORECASE),
        re.compile(r"\bwedding reception\b", re.IGNORECASE),
        re.compile(r"\bcelebration\b", re.IGNORECASE),
        re.compile(r"\blarge gathering\b", re.IGNORECASE),
        re.compile(r"\bpeople gathered\b", re.IGNORECASE),
        re.compile(r"\bunauthorized gathering\b", re.IGNORECASE),
        re.compile(r"\bgathered at (?:a|the) residence\b", re.IGNORECASE),
    ),
    "mentions_argument": (
        re.compile(r"\bargument\b", re.IGNORECASE),
        re.compile(r"\bdispute\b", re.IGNORECASE),
        re.compile(r"\bfight\b", re.IGNORECASE),
        re.compile(r"\baltercation\b", re.IGNORECASE),
        re.compile(r"\bconfrontation\b", re.IGNORECASE),
        re.compile(r"\bfeud\b", re.IGNORECASE),
    ),
    "mentions_domestic": (
        re.compile(r"\bdomestic\b", re.IGNORECASE),
        re.compile(r"\bdomestic violence\b", re.IGNORECASE),
        re.compile(r"\bdomestic dispute\b", re.IGNORECASE),
        re.compile(r"\bintimate partner\b", re.IGNORECASE),
        re.compile(r"\brelationship violence\b", re.IGNORECASE),
        re.compile(r"\bestranged\b", re.IGNORECASE),
    ),
    "mentions_bar_or_nightclub": (
        re.compile(r"\bbar\b", re.IGNORECASE),
        re.compile(r"\bnightclub\b", re.IGNORECASE),
        re.compile(r"\bnight club\b", re.IGNORECASE),
        re.compile(r"\blounge\b", re.IGNORECASE),
        re.compile(r"\btavern\b", re.IGNORECASE),
        re.compile(r"\bpub\b", re.IGNORECASE),
        re.compile(r"\bkaraoke bar\b", re.IGNORECASE),
        re.compile(r"\bclub\b", re.IGNORECASE),
    ),
    "mentions_school": EXPLICIT_SCHOOL_LOCATION_PATTERNS,
    "mentions_public_location": (
        re.compile(r"\bstreet\b", re.IGNORECASE),
        re.compile(r"\bsidewalk\b", re.IGNORECASE),
        re.compile(r"\bintersection\b", re.IGNORECASE),
        re.compile(r"\bblock\b", re.IGNORECASE),
        re.compile(r"\boutside\b", re.IGNORECASE),
        re.compile(r"\bparking lot\b", re.IGNORECASE),
        re.compile(r"\bgas station\b", re.IGNORECASE),
        re.compile(r"\bin the area\b", re.IGNORECASE),
        re.compile(r"\bnearby\b", re.IGNORECASE),
    ),
    "mentions_store_or_restaurant": (
        re.compile(r"\brestaurant\b", re.IGNORECASE),
        re.compile(r"\bdiner\b", re.IGNORECASE),
        re.compile(r"\bcafe\b", re.IGNORECASE),
        re.compile(r"\bstore\b", re.IGNORECASE),
        re.compile(r"\bgrocery\b", re.IGNORECASE),
        re.compile(r"\bmarket\b", re.IGNORECASE),
        re.compile(r"\bmall\b", re.IGNORECASE),
        re.compile(r"\bshopping center\b", re.IGNORECASE),
        re.compile(r"\bfast food\b", re.IGNORECASE),
        re.compile(r"\brestaurant patio\b", re.IGNORECASE),
    ),
    "mentions_drive_by": (
        re.compile(r"\bdrive-by\b", re.IGNORECASE),
        re.compile(r"\bdrive by\b", re.IGNORECASE),
        re.compile(r"\bshot from (?:a|the) car\b", re.IGNORECASE),
        re.compile(r"\bfired from (?:a|the) vehicle\b", re.IGNORECASE),
        re.compile(r"\bcar pulled up\b", re.IGNORECASE),
        re.compile(r"\bopened fire from (?:a|the) vehicle\b", re.IGNORECASE),
        re.compile(r"\bopened fire from vehicle\b", re.IGNORECASE),
    ),
    "mentions_street_takeover": (
        re.compile(r"\bstreet takeover\b", re.IGNORECASE),
        re.compile(r"\bside show\b", re.IGNORECASE),
        re.compile(r"\bintersection takeover\b", re.IGNORECASE),
        re.compile(r"\billegal exhibition driving\b", re.IGNORECASE),
    ),
    "mentions_family_relationship": (
        re.compile(
            r"\b(?:mother|father|son|daughter|brother|sister|uncle|aunt|cousin|nephew|niece|"
            r"stepfather|stepmother|stepson|stepdaughter|grandmother|grandfather|grandson|"
            r"granddaughter|wife|husband|girlfriend|boyfriend|partner|fiance|fiancee|"
            r"ex-wife|ex-husband|ex-girlfriend|ex-boyfriend)\b",
            re.IGNORECASE,
        ),
    ),
}

CATEGORY_PATTERNS: Final[dict[str, tuple[re.Pattern[str], ...]]] = {
    "domestic_strong": (
        re.compile(r"\bdomestic violence\b", re.IGNORECASE),
        re.compile(r"\bdomestic dispute\b", re.IGNORECASE),
        re.compile(r"\bintimate partner\b", re.IGNORECASE),
        re.compile(r"\bestranged (?:wife|husband|girlfriend|boyfriend|partner)\b", re.IGNORECASE),
        re.compile(r"\bfamily violence\b", re.IGNORECASE),
    ),
    "party_social_event": (
        re.compile(r"\bparty\b", re.IGNORECASE),
        re.compile(r"\bhouse party\b", re.IGNORECASE),
        re.compile(r"\bpool party\b", re.IGNORECASE),
        re.compile(r"\bcookout\b", re.IGNORECASE),
        re.compile(r"\bcelebration\b", re.IGNORECASE),
        re.compile(r"\bgraduation\b", re.IGNORECASE),
        re.compile(r"\bbirthday\b", re.IGNORECASE),
        re.compile(r"\bwedding\b", re.IGNORECASE),
        re.compile(r"\breception\b", re.IGNORECASE),
        re.compile(r"\bblock party\b", re.IGNORECASE),
        re.compile(r"\blarge gathering\b", re.IGNORECASE),
        re.compile(r"\bpeople gathered\b", re.IGNORECASE),
        re.compile(r"\bunauthorized gathering\b", re.IGNORECASE),
        re.compile(r"\bgathered at (?:a|the) residence\b", re.IGNORECASE),
    ),
    "nightlife_bar_district": (
        re.compile(r"\bbar\b", re.IGNORECASE),
        re.compile(r"\bnightclub\b", re.IGNORECASE),
        re.compile(r"\bnight club\b", re.IGNORECASE),
        re.compile(r"\blounge\b", re.IGNORECASE),
        re.compile(r"\btavern\b", re.IGNORECASE),
        re.compile(r"\bpub\b", re.IGNORECASE),
        re.compile(r"\bentertainment district\b", re.IGNORECASE),
    ),
    "interpersonal_dispute": (
        re.compile(r"\bargument\b", re.IGNORECASE),
        re.compile(r"\bdispute\b", re.IGNORECASE),
        re.compile(r"\baltercation\b", re.IGNORECASE),
        re.compile(r"\bfight\b", re.IGNORECASE),
        re.compile(r"\bconfrontation\b", re.IGNORECASE),
        re.compile(r"\bafter exchanging words\b", re.IGNORECASE),
    ),
    "public_event_gathering": (
        re.compile(r"\bfestival\b", re.IGNORECASE),
        re.compile(r"\bpublic event\b", re.IGNORECASE),
        re.compile(r"\bcivic center event\b", re.IGNORECASE),
        re.compile(r"\bbeach gathering\b", re.IGNORECASE),
        re.compile(r"\boceanfront\b", re.IGNORECASE),
        re.compile(r"\blarge crowd\b", re.IGNORECASE),
        re.compile(r"\bcrowds? gathered\b", re.IGNORECASE),
        re.compile(r"\bduring (?:a|the) event\b", re.IGNORECASE),
        re.compile(r"\bat (?:a|the) event\b", re.IGNORECASE),
    ),
    "gang_criminal_activity": (
        re.compile(r"\bgang[- ]related\b", re.IGNORECASE),
        re.compile(r"\bgang\b", re.IGNORECASE),
        re.compile(r"\bdrug deal\b", re.IGNORECASE),
        re.compile(r"\bretaliation\b", re.IGNORECASE),
        re.compile(r"\bcarjacking\b", re.IGNORECASE),
        re.compile(r"\barmed robbery\b", re.IGNORECASE),
        re.compile(r"\bburglary\b", re.IGNORECASE),
        re.compile(r"\bhome invasion\b", re.IGNORECASE),
        re.compile(r"\bdrive-by\b", re.IGNORECASE),
    ),
    "public_space_nonrandom": (
        re.compile(r"\bparking lot\b", re.IGNORECASE),
        re.compile(r"\bgas station\b", re.IGNORECASE),
        re.compile(r"\bpark\b", re.IGNORECASE),
        re.compile(r"\bintersection\b", re.IGNORECASE),
        re.compile(r"\bstreet corner\b", re.IGNORECASE),
        re.compile(r"\bsidewalk\b", re.IGNORECASE),
        re.compile(r"\bapartment complex\b", re.IGNORECASE),
        re.compile(r"\btargeted\b", re.IGNORECASE),
        re.compile(r"\bknown to each other\b", re.IGNORECASE),
        re.compile(r"\bstreet takeover\b", re.IGNORECASE),
    ),
    "workplace_business": (
        re.compile(r"\bcoworker\b", re.IGNORECASE),
        re.compile(r"\bco-worker\b", re.IGNORECASE),
        re.compile(r"\bemployee\b", re.IGNORECASE),
        re.compile(r"\bworkplace dispute\b", re.IGNORECASE),
        re.compile(r"\bwhile working\b", re.IGNORECASE),
        re.compile(r"\binside (?:a|the) (?:business|store|shop|office|warehouse|restaurant|barbershop)\b", re.IGNORECASE),
    ),
    "school_campus": EXPLICIT_SCHOOL_LOCATION_PATTERNS,
}

PRIVATE_PARTY_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\bprivate party\b", re.IGNORECASE),
    re.compile(r"\bhouse party\b", re.IGNORECASE),
    re.compile(r"\bpool party\b", re.IGNORECASE),
    re.compile(r"\bbirthday party\b", re.IGNORECASE),
    re.compile(r"\bgraduation party\b", re.IGNORECASE),
    re.compile(r"\bwedding reception\b", re.IGNORECASE),
    re.compile(r"\bgathered at (?:a|the) residence\b", re.IGNORECASE),
    re.compile(r"\bat (?:a|the) residence\b", re.IGNORECASE),
    re.compile(r"\bat (?:his|her|their) home\b", re.IGNORECASE),
    re.compile(r"\binside (?:a|the) home\b", re.IGNORECASE),
)

PUBLIC_TARGETING_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\btargeted\b", re.IGNORECASE),
    re.compile(r"\bknown to each other\b", re.IGNORECASE),
    re.compile(r"\bafter an argument\b", re.IGNORECASE),
    re.compile(r"\bin retaliation\b", re.IGNORECASE),
)

SUSPECT_ANCHOR_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(?:suspect|suspects|shooter|shooters|gunman|gunmen|gunwoman|gunwomen|"
    r"assailant|assailants|defendant|defendants|accused|alleged shooter|"
    r"alleged suspect|person of interest|arrested|charged)\b",
    re.IGNORECASE,
)

AGE_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(
        r"\b(?P<age>\d{1,2})-year-old\b(?=[^.!?]{0,30}\b(?:suspect|shooter|gunman|man|woman|male|female|boy|girl)\b)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:suspect|shooter|gunman|assailant|defendant|accused)\b[^.!?]{0,25}\b(?P<age>\d{1,2})-year-old\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:suspect|shooter|gunman|assailant|defendant|accused)\b[^.!?]{0,25}\bage\s+(?P<age>\d{1,2})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bage\s+(?P<age>\d{1,2})\b(?=[^.!?]{0,25}\b(?:suspect|shooter|gunman|assailant|defendant|accused)\b)",
        re.IGNORECASE,
    ),
)

GENDER_PATTERNS: Final[dict[str, tuple[re.Pattern[str], ...]]] = {
    "male": (
        re.compile(
            r"\b(?:suspect|shooter|gunman|assailant|defendant|accused|arrested|charged)\b[^.!?]{0,35}\b(?:man|male|boy|men|boys)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:man|male|boy|men|boys)\b[^.!?]{0,35}\b(?:suspect|shooter|gunman|assailant|defendant|accused)\b",
            re.IGNORECASE,
        ),
        re.compile(r"\b(?:man|male|boy|men|boys)\b[^.!?]{0,20}\b(?:was|were)\s+(?:arrested|charged|identified)\b", re.IGNORECASE),
    ),
    "female": (
        re.compile(
            r"\b(?:suspect|shooter|gunman|assailant|defendant|accused|arrested|charged)\b[^.!?]{0,35}\b(?:woman|female|girl|women|girls)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:woman|female|girl|women|girls)\b[^.!?]{0,35}\b(?:suspect|shooter|gunman|assailant|defendant|accused)\b",
            re.IGNORECASE,
        ),
        re.compile(r"\b(?:woman|female|girl|women|girls)\b[^.!?]{0,20}\b(?:was|were)\s+(?:arrested|charged|identified)\b", re.IGNORECASE),
    ),
}

RACE_PATTERNS: Final[dict[str, tuple[re.Pattern[str], ...]]] = {
    "black": (
        re.compile(r"\bblack\b[^.!?]{0,25}\b(?:suspect|shooter|gunman|assailant|man|male|woman|female|boy|girl)\b", re.IGNORECASE),
        re.compile(r"\b(?:suspect|shooter|gunman|assailant)\b[^.!?]{0,25}\bblack\b", re.IGNORECASE),
        re.compile(r"\bafrican american\b[^.!?]{0,25}\b(?:suspect|man|male|woman|female)\b", re.IGNORECASE),
    ),
    "white": (
        re.compile(r"\bwhite\b[^.!?]{0,25}\b(?:suspect|shooter|gunman|assailant|man|male|woman|female|boy|girl)\b", re.IGNORECASE),
        re.compile(r"\b(?:suspect|shooter|gunman|assailant)\b[^.!?]{0,25}\bwhite\b", re.IGNORECASE),
    ),
    "hispanic_or_latino": (
        re.compile(r"\bhispanic\b[^.!?]{0,25}\b(?:suspect|shooter|gunman|assailant|man|male|woman|female)\b", re.IGNORECASE),
        re.compile(r"\blatino\b[^.!?]{0,25}\b(?:suspect|man|male)\b", re.IGNORECASE),
        re.compile(r"\blatina\b[^.!?]{0,25}\b(?:suspect|woman|female)\b", re.IGNORECASE),
        re.compile(r"\b(?:suspect|shooter|gunman|assailant)\b[^.!?]{0,25}\b(?:hispanic|latino|latina)\b", re.IGNORECASE),
    ),
    "asian": (
        re.compile(r"\basian\b[^.!?]{0,25}\b(?:suspect|shooter|gunman|assailant|man|male|woman|female)\b", re.IGNORECASE),
        re.compile(r"\b(?:suspect|shooter|gunman|assailant)\b[^.!?]{0,25}\basian\b", re.IGNORECASE),
    ),
    "native_american": (
        re.compile(r"\bnative american\b[^.!?]{0,25}\b(?:suspect|man|male|woman|female)\b", re.IGNORECASE),
        re.compile(r"\b(?:suspect|shooter|gunman|assailant)\b[^.!?]{0,25}\bnative american\b", re.IGNORECASE),
    ),
    "middle_eastern_or_north_african": (
        re.compile(r"\bmiddle eastern\b[^.!?]{0,25}\b(?:suspect|man|male|woman|female)\b", re.IGNORECASE),
        re.compile(r"\barab\b[^.!?]{0,25}\b(?:suspect|man|male|woman|female)\b", re.IGNORECASE),
    ),
}

SUSPECT_COUNT_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(
        r"\b(?P<count>\d+|one|two|three|four|five|six)\s+(?:suspects|shooters|gunmen|gunwomen|assailants|defendants)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?P<count>\d+|one|two|three|four|five|six)\s+(?:men|women|boys|girls)\s+(?:were|was)\s+(?:arrested|charged|identified)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bpolice\s+(?:said\s+)?(?:arrested|charged)\s+(?P<count>\d+|one|two|three|four|five|six)\b",
        re.IGNORECASE,
    ),
)

NUMBER_WORDS: Final[dict[str, int]] = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
}
