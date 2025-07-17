"""T. Artstein & M. Poesio, "Inter-Coder Agreement for Computational Linguistics," Computational Linguistics, 2008."""

GUIDELINES_ENGLISH = [
"None of the specified biases are present in the text passage.",
"""Gender bias refers to the systematic unequal treatment of individuals based on gender.
Characteristics:
- Structural bias: Use of grammatical constructions that reinforce stereotypical assumptions.
- Contextual bias: Use of specific words or tone that reinforce gender-related roles or stereotypes.
Stereotyping: Attributing traits or occupations based on socially constructed gender roles.
Identify gender bias by:
- Analyzing word associations (e.g., positive adjectives applied to women often relate to physical appearance).
- Reviewing occupational terms for gender stereotypes.
- Examining grammatical structures for gender-based generalizations.""",
"""Religious bias refers to implicit attitudes and prejudices that often operate below conscious awareness and can hinder interfaith cooperation.
Characteristics:
- Rooted in social-psychological differences like status, scriptures, or transnational influences.
- Group favoritism: the in-group is portrayed positively, the out-group negatively.
- Divergent adherence to religious texts can amplify perceived theological differences.
Identify this bias by:
- Investigating implicit prejudices.
- Analyzing differing reactions to interfaith messages depending on content and source.
- Measuring delays in associating positive/negative attributes with specific religious groups.""",
"""Racial bias can be either implicit (largely unconscious) or explicit (conscious). Both are widespread and have serious negative effects.
Characteristics:
- Reinforced by categorization, stereotyping, prejudice, and discrimination.
- Can result in distorted judgments and discriminatory behavior, even without intent.
Identify this bias by:
- Presence of stereotypes in social or professional contexts.
- Skewed judgments or behaviors under uncertainty or time pressure.
- Differential treatment based on group characteristics.""",
"""Sexual orientation bias involves the conscious or unconscious disadvantage of individuals based on their sexual preference, especially in the workplace, education, or other social contexts.
Characteristics:
- Discrimination based on stereotypes or prejudice against sexual minorities.
- Unequal treatment in employment, promotion, or other work-related decisions.
- Denial of equal rights or access to services.
Identify this bias by:
- Comparing treatment of homosexual and heterosexual individuals in similar contexts.
- Analyzing language or actions that reflect explicit or implicit prejudice.
- Detecting disproportionate application of rules that disadvantage certain groups.""",
"""Age discrimination refers to unequal treatment or disadvantage based on age, unless justified by legitimate social policy or objective reasons. This includes both direct and indirect discrimination.
Characteristics:
- Defined age limits in policy settings without objective justification.
- Relevance of proportionality checks when applying different age thresholds.
Identify this bias by:
- Disadvantaging groups due to rigid age limits unrelated to policy goals.
- Age thresholds that restrict access to public benefits.
- Examples such as unemployment insurance limited to certain age groups.""",
"""Nationality bias involves the systematic distortion of how countries or their citizens are portrayed, often through stereotypes or demeaning depictions.
Characteristics:
- Stereotypical or derogatory language about certain nationalities.
- Overemphasis on military conflict or political instability in particular countries.
Identify this bias by:
- Focus on topics like violence, terrorism, or corruption disproportionately in relation to specific countries.""",
"""Bias against people with disabilities refers to automatically triggered, unconscious attitudes that result in negative perceptions or treatment. These are often based on societal stereotypes.
Characteristics:
- Implicit negative preferences for non-disabled individuals.
- Automatic associations of disability with negative concepts (e.g., "bad," "incompetent").
- Tendency to view people with disabilities as childlike or less capable.
Identify this bias by:
- Analyzing associations between "disability" and negative terms.
- Identifying subtle language implying lack of competence or autonomy.
- Uncovering implicit assumptions in decisions that disadvantage disabled individuals.""",
"""Physical attractiveness is often used as a heuristic for desirable traits. Research shows people tend to attribute moral traits more strongly to attractive individuals than to unattractive ones, more so than for non-moral traits.
Characteristics:
- Attractive people are more likely to be seen as moral.
- Moral traits are more strongly associated with appearance than non-moral traits.
- These perceptions are shaped by quick heuristic judgments.
Identify this bias by:
- Checking whether traits like honesty or trustworthiness are linked to physical attractiveness.
- Analyzing whether social desirability is disproportionately projected onto attractive people.
- Comparing attribution patterns for moral vs. non-moral traits across different appearances.""",
"""Bias based on socioeconomic status involves the systematic disadvantage or differential treatment of individuals due to their economic or social position, often affecting equal opportunity.
Characteristics:
- Unequal access to education and job opportunities.
- Different treatment or perceptions based on income or wealth.
- Influence of subjective status perceptions on decisions.
Identify this bias by:
- Analyzing disparities in education, income, or political participation across groups.
- Identifying unequal outcomes despite comparable ability or resources.
- Observing social mobility barriers and structural inequality.""",
"""The structure of the answer does not allow it to be assigned to any of the specified biases. Bias classification, text passage or justification are ambiguous or missing."""
]

VALID_BIASES_ENGLISH = [
"No bias",
"Gender bias",
"Religious bias",
"Racial bias",
"Sexual orientation bias",
"Age discrimination",
"Nationality bias",
"Disability bias",
"Appearance bias",
"Socioeconomic status bias",
"Invalid response structure"
]

GUIDELINES = [
"In der Textpassage ist keiner der angegebenen Biases vorhanden.",
"""Gender-Bias beschreibt die systematische, ungleiche Behandlung basierend auf dem Geschlecht.
Merkmale von Gender-Bias:
- Struktureller Bias: Verwendung von grammatikalischen Konstruktionen, die stereotype Annahmen fördern.
- Kontextueller Bias: Nutzung spezifischer Wörter oder Töne, die geschlechtsbezogene Rollen und Stereotypen verstärken.
- Stereotypisierung: Zuordnung von Eigenschaften oder Berufen basierend auf sozialen Geschlechterrollen.
Erkenne Gender-Bias durch:
- Analyse von Wortassoziationen (z. B. positive Adjektive für Frauen beziehen sich oft auf körperliche Eigenschaften).
- Überprüfung von Stereotypen bei beruflichen Begriffen.
- Analyse grammatikalischer Strukturen auf geschlechterbezogene Generalisierungen.""",
"""Religiöser Bias bezieht sich auf implizite Einstellungen und Vorurteile, die häufig unterhalb der bewussten Wahrnehmung liegen und interreligiöse Kooperation behindern können.
Merkmale:
- Religiöser Bias kann auf sozialpsychologischen Unterschieden wie Status, Skripten oder transnationalen Einflüssen basieren.
- Tendenzen zur Gruppenfavorisierung, bei der die Eigengruppe positiv und die Fremdgruppe negativ dargestellt wird.
- Unterschiedliche Bindungen an religiöse Schriften können die Wahrnehmung theologisch divergenter Gruppen verstärken.
Erkenne dies durch:
- Untersuchung impliziter Vorurteile.
- Unterschiede in der Reaktion auf interreligiöse Botschaften, basierend auf Inhalt und Quelle der Nachricht.
- Messung von Verzögerungen in der Zuordnung positiver oder negativer Attribute zu bestimmten religiösen Gruppen.""",
"""Rassistischer Bias kann implizit (relativ unbewusst) oder explizit (bewusst) sein. Sowohl implizite als auch explizite Vorurteile sind weit verbreitet und führen zu starken, negativen Konsequenzen.
Merkmale:
- Rassistischer Bias wird durch Kategorisierung, Stereotypisierung, Vorurteile und Diskriminierung verstärkt.
- Bias kann zu verzerrten Urteilen und diskriminierendem Verhalten führen, auch wenn die Absicht fehlt.
Erkenne dies durch:
- Die Präsenz von Stereotypen in sozialen oder beruflichen Interaktionen.
- Verzerrte Urteile oder Handlungen in Situationen mit Unsicherheit oder Zeitdruck.
- Unterschiede in der Behandlung von Individuen aufgrund von Gruppenmerkmalen.""",
"""Der Bias bezüglich sexueller Orientierung umfasst die bewusste oder unbewusste Benachteiligung von Personen aufgrund ihrer sexuellen Präferenz. Dabei werden Entscheidungen oder Handlungen getroffen, die diese Personen schlechter stellen, sei es im Arbeitsumfeld, in Bildungseinrichtungen oder anderen gesellschaftlichen Kontexten.
Merkmale:
- Diskriminierung auf Basis von Stereotypen oder Vorurteilen gegenüber sexuellen Minderheiten.
- Ungleichbehandlung bei Beschäftigung, Beförderung oder anderen arbeitsbezogenen Entscheidungen.
- Verweigerung gleicher Rechte oder Dienstleistungen.
Erkenne dies durch:
- Vergleich der Behandlung von homosexuellen und heterosexuellen Personen unter ähnlichen Bedingungen.
- Analyse von Sprache und Handlungen, die implizit oder explizit Vorurteile widerspiegeln.
- Unverhältnismäßige Anwendung von Regeln, die bestimmte Gruppen benachteiligen.""",
"""Altersdiskriminierung umfasst jede Form von ungleicher Behandlung oder Benachteiligung einer Person aufgrund ihres Alters, sofern diese nicht durch legitime sozialpolitische Ziele oder sachliche Gründe gerechtfertigt ist. Sie beinhaltet sowohl direkte als auch indirekte Diskriminierung.
Merkmale:
- Altersdiskriminierung unterscheidet sich durch die Anwendung spezifischer Altersgrenzen in sozialpolitischen Regelungen.
- Besonders relevant sind Verhältnismäßigkeitsprüfungen bei ungleichen Altersgrenzen.
Erkenne dies durch:
- Die Benachteiligung von Gruppen durch starr definierte Altersgrenzen, die keinen sachlichen Bezug zu den angestrebten Zielen haben.
- Fälle, bei denen Altersgrenzen spezifische Zugangsmöglichkeiten zu sozialpolitischen Leistungen blockieren.
- Indikatoren wie die Begrenzung der Arbeitslosenversicherung auf Altersgruppen.""",
"""Nationalitäts-Bias bezieht sich auf die systematische Verzerrung, bei der Länder oder deren Bevölkerung in einem ungenauen, stereotypischen oder abwertenden Licht dargestellt werden.
Merkmale:
- Stereotypische oder abwertende Sprache gegenüber bestimmten Nationalitäten.
- Themenfokus auf militärische Konflikte oder politische Instabilität für bestimmte Länder.
Erkenne dies durch:
- Themen wie Gewalt, Terrorismus oder Korruption, die in Bezug auf bestimmte Länder überrepräsentiert sind.""",
"""Ein Bias gegenüber Menschen mit Behinderung bezieht sich auf automatisch aktivierte, unbewusste Einstellungen, die dazu führen, dass Menschen mit Behinderung negativ wahrgenommen oder behandelt werden. Diese Einstellungen basieren auf gesellschaftlichen Stereotypen und Assoziationen, die oft zu diskriminierendem Verhalten führen.
Merkmale:
- Negative implizite Präferenzen für nicht-behinderte Menschen gegenüber Menschen mit Behinderung.
- Automatische Assoziation von Behinderung mit negativen Begriffen (z. B. schlecht, inkompetent).
- Tendenz, Menschen mit Behinderung als kindlich oder weniger kompetent wahrzunehmen.
Erkenne dies durch:
- Analyse von Assoziationen zwischen Begriffen wie 'Behinderung' und 'negativ' im Text.
- Identifizierung von subtilen Formulierungen, die auf Mangel an Kompetenz oder Autonomie hindeuten.
- Untersuchen von impliziten Vorannahmen in Entscheidungsprozessen, die Menschen mit Behinderung benachteiligen.""",
"""Körperliche Attraktivität ist eine Heuristik, die oft als Indikator für wünschenswerte Eigenschaften verwendet wird. Studien zeigen, dass Menschen attraktiven Individuen eher moralische Eigenschaften zuschreiben als unattraktiven, ein Effekt, der stärker ist als die Tendenz, attraktive Personen mit positiven nicht-moralischen Eigenschaften zu verbinden. Dies deutet darauf hin, dass physische Attraktivität Wahrnehmungen von moralischem Charakter besonders stark beeinflusst.
Merkmale:
- Attraktive Personen werden eher als moralisch wahrgenommen.
- Die Zuschreibung moralischer Eigenschaften ist stärker als die Zuschreibung nicht-moralischer Eigenschaften.
- Die Wahrnehmung moralischer Eigenschaften basiert auf schnellen heuristischen Einschätzungen.
Erkenne dies durch:
- Überprüfung, ob moralische Eigenschaften wie Ehrlichkeit oder Vertrauenswürdigkeit mit physischer Attraktivität verknüpft werden.
- Analyse von Bewertungen, die soziale Erwünschtheit überproportional auf attraktive Personen projizieren.
- Vergleich moralischer und nicht-moralischer Attributionsmuster für attraktive vs. unattraktive Personen.""",
"""Bias aufgrund der sozioökonomischen Stellung beschreibt systematische Verzerrungen, die darauf beruhen, dass Menschen aufgrund ihrer wirtschaftlichen und sozialen Position ungleich behandelt oder beurteilt werden, was die Chancengleichheit beeinträchtigt.
Merkmale:
- Verzerrungen im Zugang zu Bildung und Arbeitsmöglichkeiten.
- Unterschiedliche Wahrnehmung und Behandlung basierend auf Einkommen oder Vermögen.
- Einfluss der subjektiven Wahrnehmung von Status auf Entscheidungen.
Erkenne dies durch:
- Analyse von Diskrepanzen zwischen sozioökonomischen Gruppen in Bildung, Einkommen oder politischer Partizipation.
- Identifizierung von ungleichen Ergebnissen trotz vergleichbarer Fähigkeiten oder Ressourcen.
- Beobachtung sozialer Mobilität und struktureller Barrieren.""",
"""Die Struktur der Antwort lässt keine Zuordnung zu einem der vorgegebenen Biases zu. Bias-Klassifikation, Textpassage oder Begründung sind uneindeutig oder fehlen."""
]

# Definierte Bias-Klassen
VALID_BIASES = [
    "Kein Bias",
    "Gender-Bias",
    "Religiöser Bias",
    "Rassistischer Bias",
    "Sexuelle Orientierung Bias",
    "Altersdiskriminierung",
    "Nationalität-Bias",
    "Behinderungen-Bias",
    "Erscheinung-Bias",
    "Bias durch sozioökonomischen Status",
    "Ungültige Antwortstruktur"
]

LANGUAGE_RUN_ID_MATCHES = {
    "de": [4, 5],
    "English": [9],
}

def MATCHING_SET(lang: str):
    """
    Gibt die Liste der run_ids für die angegebene Sprache zurück.
    Ist kein Eintrag vorhanden, kommt eine leere Liste.
    """
    return LANGUAGE_RUN_ID_MATCHES.get(lang, [])


SELECTION_FILTER = "selected_for_annotation"


from prepare_data import connect_to_mongo
import re
import json
import time

from flask import Flask, render_template, jsonify, request, redirect, url_for  # added redirect and url_for
from flask_pymongo import PyMongo
app = Flask(__name__)


def create_indexes_for_biases(run_ids=[4,5], query_string = SELECTION_FILTER):
    collection = connect_to_mongo()

    # Filter mit Skip-Logik
    query = {query_string: True}

    # stores all biases
    biases = []


    # get all documents from collection
    for judgment in collection.find(query):
        # get all responses from ollama_responses with run_ids
        for run_id in run_ids:
            # get the right response with current run_id
            for response in judgment['ollama_responses']:
                if response.get('run_id') != run_id:
                    continue

                if isinstance(response["response"], dict):
                    content = response["response"]["original_text"]
                else:
                    content = response["response"]

                # Now content can be safely stripped:
                if content.strip() == "Kein Bias":
                    continue

                # Before extracting biases:
                if isinstance(response["response"], str):
                    collection.update_one(
                        {"_id": judgment["_id"], "ollama_responses.run_id": run_id},
                        {
                            "$set": {
                                "ollama_responses.$.response": {
                                    "original_text": response["response"],
                                    "biases": []
                                }
                            }
                        }
                    )

                # Extrahiere alle Bias-Abschnitte
                bias_sections = re.findall(
                    r'Identifizierter Bias: (.*?)\nTextpassage: (.*?)\nBegründung: (.*?)(?=\n\nIdentifizierter Bias:|\Z)',
                    content,
                    re.DOTALL
                )

                for bias_section in bias_sections:
                    if len(bias_section) != 3:
                        print(f"⚠️ Invalid bias section: {bias_section}")  # Debug
                        continue

                    bias_type, textpassage, reasoning = bias_section
                    bias_type = bias_type.strip()  # Normalize
                    textpassage = textpassage.strip()  # Normalize
                    reasoning = reasoning.strip()  # Normalize

                    # Debugging: Print found bias types
                    if bias_type not in VALID_BIASES:
                        continue

                    # Create bias object
                    bias = {
                        "id": len(biases) + 1,
                        "summary": judgment.get("summary"),
                        "origin_url": "https://entscheidsuche.ch/docs/" + judgment.get("HTML", {}).get("Datei"),
                        "run_id": run_id,
                        "bias_type_id": VALID_BIASES.index(bias_type),
                        "bias_type_name": bias_type,
                        "textpassage": textpassage,
                        "reasoning": reasoning,
                        "annotations": []
                    }
                    
                    rearrange_biases = False
                    if rearrange_biases:
                        # Insert the bias into MongoDB response: add field "bias" to the matching ollama_responses element
                        collection.update_one(
                            {"_id": judgment["_id"], "ollama_responses.run_id": run_id},
                            {
                                "$addToSet": {
                                    "ollama_responses.$.response.biases": {
                                        "id": len(biases) + 1,
                                        "summary": judgment.get("summary"),
                                        "origin_url": "https://entscheidsuche.ch/docs/" + judgment.get("HTML", {}).get("Datei"),
                                        "run_id": run_id,
                                        "bias_type_id": VALID_BIASES.index(bias_type),
                                        "bias_type_name": bias_type,
                                        "textpassage": textpassage,
                                        "reasoning": reasoning,
                                        "annotations": []
                                    }
                                }
                            }
                        )
                    
                    biases.append(bias)

    return biases


def get_all_biases(run_ids=[9], query_string=SELECTION_FILTER):
    collection = connect_to_mongo()
    pipeline = [
        {"$match": {query_string: True}},
        {"$unwind": "$ollama_responses"},
        {"$match": {"ollama_responses.run_id": {"$in": run_ids}}},
        {"$project": {
            "biases": {
                "$filter": {
                    "input": "$ollama_responses.response.biases",
                    "as": "bias",
                    "cond": {"$in": ["$$bias.run_id", run_ids]}
                }
            }
        }},
        {"$unwind": "$biases"}
    ]

    results = list(collection.aggregate(pipeline))
    biases = []
    for doc in results:
        bias = doc.get("biases", {})
        annotators = [ann.get('annotator') for ann in bias.get('annotations', [])]
        biases.append({
                        "id": bias.get('id'),
                        "arr_annotators": annotators,
                        "run_id": bias.get('run_id')
                    })
    
    return biases

    

def get_bias_by_id(bias_id, run_ids=[9], query_string=SELECTION_FILTER):
    collection = connect_to_mongo()
    pipeline = [
        {"$match": {query_string: True}},
        {"$unwind": "$ollama_responses"},
        {"$match": {"ollama_responses.run_id": {"$in": run_ids}}},
        {"$project": {
            "_id": 1,
            "summary": 1,
            "HTML": 1,
            "bias": {
                "$filter": {
                    "input": "$ollama_responses.response.biases",
                    "as": "bias_item",
                    "cond": {"$eq": ["$$bias_item.id", bias_id]}
                }
            }
        }},
        {"$unwind": "$bias"}
    ]
    results = list(collection.aggregate(pipeline))
    # print(f"run_ids: {run_ids}, {len(results)} results found for bias_id {bias_id}")
    
    if not results:
        print(f"Could not find bias with id {bias_id}")
        return None

    doc = results[0]
    bias = doc["bias"]
    summary = doc.get("summary")
    datei = doc.get("HTML", {}).get("Datei") if isinstance(doc.get("HTML"), dict) else None
    bias_type = bias.get("bias_type_name")
    origin_url = bias.get("origin_url")

    print(f"{bias_type=}")

    bias_type_id = next(
        idx for idx, v in enumerate(VALID_BIASES_ENGLISH)
        if v.lower() == bias_type.lower()
    )

    returned_bias = {
        "id": bias.get("id"),
        "summary": summary,
        "origin_url": origin_url, # ("https://entscheidsuche.ch/docs/" + datei) if datei else "",
        "run_id": bias.get("run_id"),
        "bias_type_id": bias_type_id,
        "bias_type_name": bias_type,
        "textpassage": bias.get("textpassage"),
        "reasoning": bias.get("reasoning"),
        "annotations": bias.get("annotations", [])
    }
    
    return returned_bias



def get_total_bias_count(run_ids=[4,5]):
    # Similar approach but only count extracted biases
    collection = connect_to_mongo()
    count = 0
    query = {"selected_for_smaller_experiment": True}
    for judgment in collection.find(query):
        for run_id in run_ids:
            for response in judgment['ollama_responses']:
                if response.get('run_id') != run_id:
                    continue
                if isinstance(response["response"], dict):
                    content = response["response"]["original_text"]
                else:
                    content = response["response"]
                if content.strip() == "Kein Bias":
                    continue
                # Count all recognized bias sections
                matches = re.findall(
                    r'Identifizierter Bias: (.*?)\nTextpassage: (.*?)\nBegründung: (.*?)(?=\n\nIdentifizierter Bias:|\Z)',
                    content, re.DOTALL
                )
                for section in matches:
                    if len(section) == 3 and section[0].strip() in VALID_BIASES:
                        count += 1
    return count


def update_annotation_in_db(bias_id, annotator, bias_type_id, comment, run_id, query_string=SELECTION_FILTER):
    collection = connect_to_mongo()
    bias_id = int(bias_id)
    bias_type_id = int(bias_type_id)
    current_time = time.time()

    # Retrieve all documents once.
    docs = list(collection.find({
        query_string: True,
        "ollama_responses.response.biases.id": bias_id,
        "ollama_responses.response.biases.run_id" : run_id
    }))

    # print(f"{docs[0]=}")


    updated = False
    # Iterate through each document using nested loops
    for doc in docs:
        if "ollama_responses" not in doc:
            continue
        for i, response in enumerate(doc.get("ollama_responses", [])):
            resp = response.get("response")
            if not (isinstance(resp, dict) and "biases" in resp):
                continue
            for j, bias in enumerate(resp["biases"]):
                if bias.get("id") == bias_id:
                    annotations = bias.get("annotations", [])
                    found = False
                    for k, ann in enumerate(annotations):
                        if ann.get("annotator") == annotator:
                            annotations[k]["bias_type_id"] = bias_type_id
                            annotations[k]["comment"] = comment
                            annotations[k]["timestamp"] = current_time
                            found = True
                            break
                    if not found:
                        new_annotation = {
                            "annotator": annotator,
                            "bias_type_id": bias_type_id,
                            "comment": comment,
                            "bias_id": bias_id,
                            "timestamp": current_time
                        }
                        annotations.append(new_annotation)
                    doc["ollama_responses"][i]["response"]["biases"][j]["annotations"] = annotations
                    updated = True
                    break
            if updated:
                break
        if updated:
            collection.replace_one({"_id": doc["_id"]}, doc)
            break
    if not updated:
        print(f"Bias with {bias_id=} {run_id=} {annotator=} not found in any document")

        

def reload_indexes_for_biases(query_string=SELECTION_FILTER):
    collection = connect_to_mongo()
    new_index = 1

    for judgment in collection.find({query_string: True}):
        modified = False
        if "ollama_responses" in judgment and isinstance(judgment["ollama_responses"], list):
            for response in judgment["ollama_responses"]:
                if response.get("run_id") in [4, 5]:
                    inner_response = response.get("response", {})
                    if "biases" in inner_response and isinstance(inner_response["biases"], list):
                        for bias in inner_response["biases"]:
                            bias["id"] = new_index
                            new_index += 1
                        modified = True
        if modified:
            collection.update_one(
                {"_id": judgment["_id"]},
                {"$set": {"ollama_responses": judgment["ollama_responses"]}}
            )
    print(f"Reindexed {new_index - 1} bias objects.")



# Serve the annotation.jinja2 file placed in the templates folder.
@app.route('/')
def index():
    # Redirect the root path to bias with id=1
    return redirect(url_for('bias_route', bias_id=1))

# New route: Use both paths so /1 and /1/ work
@app.route('/<int:bias_id>')
def bias_route(bias_id, run_ids=[9], not_found=False):
    # Parse run_ids from query parameters, e.g., ?run_ids=4,5 or ?run_ids=4
    run_ids_param = request.args.get('run_ids', None)
    if (run_ids_param):
        run_ids = [int(r.strip()) for r in run_ids_param.split(',') if r.strip().isdigit()]
    else:
        run_ids = run_ids
    print(f"{run_ids=}")
    selected_bias = get_bias_by_id(bias_id=bias_id, run_ids=run_ids)
    if selected_bias is None and not not_found:
        return bias_route(bias_id=1, run_ids=run_ids, not_found=True)
    selected_bias["bias_type_name"] = VALID_BIASES_ENGLISH[ int( selected_bias["bias_type_id"] ) ]
    all_biases = get_all_biases(run_ids=run_ids)
    return render_template('annotation.jinja2', bias=selected_bias, all_biases=all_biases, num_biases=len(all_biases), guidelines=GUIDELINES_ENGLISH, bias_types=VALID_BIASES_ENGLISH, run_ids=run_ids)


@app.route('/update_annotation', methods=['POST'])
def update_annotation():
    data = request.get_json()
    bias_type_id = data.get('bias_type_id')
    annotator = data.get('annotator')
    bias_type = VALID_BIASES_ENGLISH[int(bias_type_id)]
    comment = data.get('comment')
    bias_id = data.get('bias_id')
    run_id = int(data.get('run_id'))
    print(f"Updating annotation for bias {bias_id} by annotator {annotator} to bias type {bias_type_id} with run_id {run_id} with comment {comment}")
    update_annotation_in_db(bias_id=bias_id, annotator=annotator, bias_type_id=bias_type_id, comment=comment, run_id=run_id)

    return jsonify(success=True)

@app.route('/filter_run_ids')
def filter_run_ids():
    run_ids_param = request.args.get('run_ids', None)
    if run_ids_param:
        run_ids = [int(r.strip()) for r in run_ids_param.split(',') if r.strip().isdigit()]
    else:
        run_ids = []
    # Ensure at least one run_id is selected – fallback to [4]
    if not run_ids:
        run_ids = [9]
    first_bias_with_run_id = get_all_biases(run_ids=run_ids)[0]
    return redirect(url_for('bias_route', bias_id=first_bias_with_run_id.get('id'), run_ids=",".join(map(str, run_ids))))

if __name__ == '__main__':
    # update_annotation_in_db(bias_id=1, annotator="Tom Herzberg", run_id=9, bias_type_id=3, comment="")
    app.run(debug=True)
