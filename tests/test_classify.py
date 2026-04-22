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
