from gva_pipeline.classify import classify_incident


def test_domestic_classification() -> None:
    text = (
        "Police said the shooting stemmed from a domestic violence dispute. "
        "The suspect opened fire after an argument with his estranged wife."
    )
    result = classify_incident(text)
    assert result.category == "domestic_family"
    assert result.confidence >= 0.88


def test_party_social_classification() -> None:
    text = (
        "Investigators said the gunfire erupted during a large birthday party. "
        "Dozens of people had gathered for the celebration."
    )
    result = classify_incident(text)
    assert result.category == "party_social_event"


def test_nightlife_classification() -> None:
    text = (
        "The shooting happened outside a downtown bar and nightclub district. "
        "Witnesses said patrons were leaving the lounge at closing time."
    )
    result = classify_incident(text)
    assert result.category == "nightlife_bar_district"


def test_house_party_classifies_as_party_social_event() -> None:
    text = (
        "Police said the shooting happened at a house party late Saturday night. "
        "Several guests ran from the residence after gunfire erupted."
    )
    result = classify_incident(text)
    assert result.category == "party_social_event"


def test_large_gathering_at_residence_classifies_as_party_social_event() -> None:
    text = (
        "Officers responded after gunfire broke out during a large gathering. "
        "Witnesses said dozens of people had gathered at a residence."
    )
    result = classify_incident(text)
    assert result.category == "party_social_event"


def test_students_at_private_party_do_not_classify_as_school_campus() -> None:
    text = (
        "Police said the victims were high school students attending a private party at a home. "
        "Investigators said the shooting happened during an unauthorized gathering at the residence."
    )
    result = classify_incident(text)
    assert result.category == "party_social_event"


def test_explicit_on_campus_incident_still_classifies_as_school_campus() -> None:
    text = (
        "The shooting happened on campus near a dormitory after students returned from an event. "
        "University police responded to the school grounds within minutes."
    )
    result = classify_incident(text)
    assert result.category == "school_campus"


def test_clear_domestic_family_case_does_not_regress() -> None:
    text = (
        "Police said the suspect opened fire after a domestic dispute with his estranged wife. "
        "Family members told investigators the violence happened inside the home."
    )
    result = classify_incident(text)
    assert result.category == "domestic_family"


def test_public_event_gathering_classification() -> None:
    text = (
        "Police said gunfire erupted during a public event at the civic center. "
        "A large crowd ran for cover as officers secured the area."
    )
    result = classify_incident(text)
    assert result.category == "public_event_gathering"
    assert result.matched_rule == "public_event_terms"


def test_festival_shooting_classifies_correctly() -> None:
    text = (
        "Investigators said the shooting happened during a festival downtown. "
        "Crowds gathered near the stage before the gunfire started."
    )
    result = classify_incident(text)
    assert result.category == "public_event_gathering"


def test_oceanfront_shooting_classifies_correctly() -> None:
    text = (
        "Officers said the shooting happened along the oceanfront during a beach gathering. "
        "Witnesses described a large crowd in the area."
    )
    result = classify_incident(text)
    assert result.category == "public_event_gathering"


def test_drive_by_overrides_workplace_false_positive() -> None:
    text = (
        "Police said an employee was standing outside the store when a car pulled up. "
        "Someone opened fire from the vehicle before speeding away."
    )
    result = classify_incident(text)
    assert result.category == "public_space_nonrandom"
    assert result.matched_rule == "drive_by_public_nonrandom"


def test_near_business_does_not_trigger_workplace() -> None:
    text = (
        "Police said the shooting happened on a sidewalk near a business district and around several stores. "
        "Investigators do not believe anyone involved was working at the time."
    )
    result = classify_incident(text)
    assert result.category == "public_space_nonrandom"


def test_multi_victim_street_shooting_classifies_as_public_space_nonrandom() -> None:
    text = (
        "Police said gunfire erupted in the street, leaving several people wounded. "
        "Investigators have not identified any relationship between the victims and the shooter."
    )
    result = classify_incident(text, victims_killed=0, victims_injured=4)
    assert result.category == "public_space_nonrandom"
    assert result.matched_rule == "multi_victim_public_location_nonrandom"


def test_group_shot_on_sidewalk_classifies_as_public_space_nonrandom() -> None:
    text = (
        "Four people were shot on the sidewalk when gunfire broke out late Saturday. "
        "Police said no clear motive or relationship among those involved has been identified."
    )
    result = classify_incident(text, victims_killed=0, victims_injured=4)
    assert result.category == "public_space_nonrandom"


def test_gas_station_shooting_classifies_as_public_space_nonrandom() -> None:
    text = (
        "Police said five people were shot at a gas station late Saturday night. "
        "Investigators said they have not identified a motive or relationship among those involved."
    )
    result = classify_incident(text, victims_killed=0, victims_injured=5)
    assert result.category == "public_space_nonrandom"


def test_thin_multi_victim_article_still_classifies_as_public_multi_victim_unclear() -> None:
    text = (
        "Police said multiple people were shot late Saturday night. "
        "Investigators have not identified a clear motive or relationship among those involved."
    )
    result = classify_incident(text, victims_killed=0, victims_injured=4)
    assert result.category == "public_multi_victim_unclear"
    assert result.matched_rule == "public_multi_victim_fallback"


def test_school_adjacent_words_without_explicit_location_do_not_trigger_school_campus() -> None:
    text = (
        "Police said high school students were injured after prom at a house party. "
        "Investigators said the shooting happened at a private home, not on school property."
    )
    result = classify_incident(text)
    assert result.category != "school_campus"
