import config from 'config';
import fs from 'fs';
import fetch from 'isomorphic-fetch';
import moment from 'moment';
import PromisePool from 'es6-promise-pool';

const COMMIT_CHANGES = true;
const CONCURRENT_FETCHES = 15;

let oclServerUrl = config.get("OCL.server");
let sourcePath = config.get("OCL.sourcePath");
let sourcePathWithVersion = ""; // need to look this up first, before we get any concepts.
let collectionPath = config.get("OCL.collectionPath");

function parseFileContents(str) {
    const lines = str.split(/\r?\n/);
    let ret = [];
    lines.forEach(function(it) {
        let parsed = parseInt(it);
        if (parsed) {
            ret.push(parsed);
        }
        else {
            console.log(`Could not parse: ${it}`)
        }
    });
    return ret;
}

function logOclConnectionError(error) {
    console.log("Error connecting to OCL");
    if (error) {
        console.log(error);
    }
    process.exit();
}

function logError(error) {
    console.log("Unknown Error");
    if (error) {
        console.log(error);
    }
}

// returns concept ids
function conceptIdsRelatedTo(concept) {
    if (!concept.mappings) {
        return [];
    }
    return concept.mappings
            .filter(m => { return m.to_source_url === sourcePath; })
            .map(m => { return m.to_concept_code; });
}

let conceptFile = config.get("local.conceptFile");
try {
    fs.accessSync(conceptFile, fs.constants.F_OK)
}
catch (err) {
    console.log(`Cannot read file: ${conceptFile}`);
    console.log(err);
    process.exit();
}

let apiToken = config.get("OCL.apiToken");
if (apiToken === "NEED TO SPECIFY IN local.json FILE IN THIS FOLDER") {
    console.log('create a config/local.json file with {"OCL":{"apiToken":"your-token"}}');
    process.exit();
}

// returns a simple array of strings (which are reference.expression)
function getCurrentReferences() {
    const url = oclServerUrl + collectionPath + "references?limit=20000";
    console.log("fetching " + url);
    return fetch(url, { headers: { "Authorization": `Token ${apiToken}` } })
            .then(response => {
                if (response.status >= 200 && response.status < 300) {
                    return response.json();
                }
                else {
                    logOclConnectionError(response.status + " " + response.statusText);
                }
            }, logOclConnectionError)
            .then(references => {
                return references.map(r => r.expression);
            })
}

function determineLatestSourceVersion() {
    const url = oclServerUrl + sourcePath + "versions"; // eventually add ?released=true
    console.log("fetching " + url);
    return fetch(url, { headers: { "Authorization": `Token ${apiToken}` } })
            .then((response) => {
                if (response.status >= 200 && response.status < 300) {
                    return response.json();
                }
                else {
                    logOclConnectionError(response.status + " " + response.statusText);
                }
            }, logOclConnectionError)
            .then(versions => {
                // I don't see a proper way to find the "latest released" version in the API, so this is a hack
                var latestVersion = versions.filter(item => {
                    return item.id !== 'HEAD'
                }).sort((a, b) => {
                    return moment(a.created_on).isBefore(b.created_on) ? 1 : -1;
                })[0];
                if (!latestVersion) {
                    console.log(`Cannot find latest released version of ${url}. Using HEAD instead`);
                    latestVersion = versions.filter(item => item.id === 'HEAD')[0];
                }
                sourcePathWithVersion = latestVersion.version_url;
                console.log(`Using version: ${sourcePathWithVersion} created ${moment(latestVersion.created_on).fromNow()}`);
                console.log();
                return latestVersion;
            });
}

function fetchConcept(conceptId) {
    const url = oclServerUrl + sourcePathWithVersion + "concepts/" + conceptId + "?includeMappings=true";
    console.log("fetching " + url);
    return fetch(url, { headers: { "Authorization": `Token ${apiToken}` } })
            .then((response) => {
                if (response.status >= 200 && response.status < 300) {
                    return response.json();
                }
                else {
                    logOclConnectionError(response.status + " " + response.statusText);
                }
            }, logOclConnectionError)
            .catch(err => {
                console.log(`Error fetching ${url}`);
                console.log(err);
                process.exit();
            })
}

let conceptsToHandle = parseFileContents(fs.readFileSync(conceptFile, {encoding: 'ascii'}));
let resultConcepts = {};
console.log(`${conceptFile} refers to ${conceptsToHandle.length} concept(s):`);
console.log(conceptsToHandle);
console.log();

// we need to avoid doing hundreds of fetches simultaneously and getting blocked by the server, so we will dynamically
// add to conceptsToHandle (and we'll handle them one at a time via PromisePool)
function producerFor(array, functionToCall) {
    return function() {
        console.log(`${array.length} left to handle`);
        const item = array.shift();
        if (item) {
            return functionToCall(item);
        } else {
            return null;
        }
    }
}

function handleRemainingConcepts() {
    console.log(`${conceptsToHandle.length} left to handle`);
    if (conceptsToHandle.length > CONCURRENT_FETCHES) {
        console.log("Handling batch of " + CONCURRENT_FETCHES);
        return Promise.all(conceptsToHandle.splice(0, CONCURRENT_FETCHES)
                                   .map(conceptId => handleConceptId(conceptId))
        ).then(handleRemainingConcepts);
    }
    else {
        const conceptId = conceptsToHandle.shift();
        if (conceptId) {
            return handleConceptId(conceptId).then(handleRemainingConcepts);
        }
        else {
            return Promise.resolve();
        }
    }
}

function handleConceptId(conceptId) {
    if (resultConcepts[conceptId]) {
        console.log("using already-fetched " + conceptId);
        return Promise.resolve(resultConcepts[conceptId]);
    }
    return fetchConcept(conceptId).then(concept => {
        resultConcepts[concept.id] = concept;
        let relatedIds = conceptIdsRelatedTo(concept);
        console.log(`got back ${concept.id} which has ${relatedIds.length} related concepts`);
        relatedIds.forEach(id => conceptsToHandle.push(id));
        return concept;
    })
            .catch(err => {
                console.log(`Error fetching ${conceptId}`);
                console.log(err);
                process.exit();
            })
}

function addReferencesToCollection(references) {
    if (references && references.length) {
        const url = oclServerUrl + collectionPath + "references";
        const data = {
            data: {
                expressions: references
            }
        };
        console.log("PUT to " + url);
        return fetch(url, {
            method: "PUT",
            body: JSON.stringify(data),
            headers: {
                "Authorization": `Token ${apiToken}`,
                "Content-Type": "application/json"
            }
        })
                .then(response => {
                    if (response.status >= 200 && response.status < 300) {
                        return response.json();
                    }
                    else {
                        logOclConnectionError(response.status + " " + response.statusText);
                    }
                }, logOclConnectionError)
                .then(messages => {
                    console.log(messages);
                    return messages;
                });
    }
    else {
        console.log("No references to add");
        return Promise.resolve([]);
    }
}

function deleteReferencesFromCollection(references) {
    if (references && references.length) {
        const url = oclServerUrl + collectionPath + "references";
        const data = {
            references: references
        };
        console.log("DELETE from " + url);
        return fetch(url, {
            method: "DELETE",
            body: JSON.stringify(data),
            headers: {
                "Authorization": `Token ${apiToken}`,
                "Content-Type": "application/json"
            }
        })
                .then(response => {
                    if (response.status >= 200 && response.status < 300) {
                        return response.json();
                    }
                    else {
                        logOclConnectionError(response.status + " " + response.statusText);
                    }
                }, logOclConnectionError)
                .then(messages => {
                    console.log(messages);
                    return messages;
                });
    }
    else {
        console.log("No references to delete");
        return Promise.resolve([]);
    }
}

Promise.all([
        getCurrentReferences(),
        determineLatestSourceVersion()
]).then(results => {
    let currentReferences = results[0];
    let latestSourceVersion = results[1]; // not actually used
    console.log(`Latest version ${latestSourceVersion} ... collection has ${currentReferences.length} references (before). Ready to start fetching concepts`);

    // const pool = new PromisePool(producerFor(conceptsToHandle, handleConceptId), CONCURRENT_FETCHES);
    
    // pool.start().then(() => {
    handleRemainingConcepts().then(() => {
        console.log("=== Interpretation ===");
        let references = [];
        for (let conceptId in resultConcepts) {
            let concept = resultConcepts[conceptId];
            references.push(concept.version_url); // reference this specific version of the concept
            console.log("Concept: " + concept.display_name);
            if (concept.mappings) {
                concept.mappings
                // .filter(m => { return m.to_source_url === sourcePath; })
                        .forEach(m => {
                            references.push(m.url);
                            console.log(`Mapping: ${m.from_concept_url} ${m.map_type} ${m.to_concept_url || (m.to_source_url + m.to_concept_code)}`);
                        })
            }
        }
        console.log("=== References ===");
        console.log(references);

        let toAdd = [];
        let alreadyThere = [];
        references.forEach(r => {
            if (currentReferences.includes(r)) {
                alreadyThere.push(r);
            }
            else {
                toAdd.push(r);
            }
        });
        let toDelete = currentReferences.filter(r => !alreadyThere.includes(r));

        console.log("Adding " + toAdd.length);
        console.log("Deleting " + toDelete.length);

        if (COMMIT_CHANGES) {
            addReferencesToCollection(toAdd).then(() => {
                deleteReferencesFromCollection(toDelete);
            });
        }
        else {
            console.log("Just testing, not committing changes");
        }
    });
});