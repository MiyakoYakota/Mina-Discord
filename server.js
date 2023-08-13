const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const _ = require('lodash');
const uuid = require('uuid');
const flat = require('flat')
const moment = require('moment');

const app = express();

app.use(bodyParser.json({ limit: '50mb' }));

const fingerprints = {
    "member": ['user', 'roles', 'nick', 'joined_at', 'premium_since', 'deaf', 'mute', 'pending','communication_disabled_until'],
    "user": ['username', 'public_flags', 'global_name', 'display_name', 'discriminator', 'bot', 'avatar'],
    "guild": ['threads', 'stickers', 'stage_instances', 'roles', 'properties', 'premium_subscription_count', 'member_count', 'lazy', 'large', 'joined_at', 'channels'],
    "role": ['unicode_emoji', 'tags', 'positions', 'name', 'mentionable', 'managed', 'icon', 'hoist', 'flags', 'color'],
    "presence": ['user', 'status', 'game', 'client_status', 'broadcast', 'activities'],
    "message": ['embeds', 'channel_id', 'guild_id', 'tts', 'referenced_message', 'message_reference', 'mentions', 'mention_everyone']
}

function isObjectArray(arr) {
    return Array.isArray(arr) && arr.length > 0 && typeof arr[0] === 'object';
}

function calculateMatchingPercentage(obj1, obj2) {
    // Calculate what % of obj1 has keys from obj2's list
    const obj1Keys = Object.keys(obj1);

    let matchingKeys = 0;

    obj2.forEach((key) => {
        if (obj1Keys.includes(key)) {
            matchingKeys += 1
        }
    })

    return matchingKeys / obj2.length;
}

function findBestMatch(data) {
    let bestMatchKey = null;
    let bestMatchPercentage = 0;

    for (const key in fingerprints) {
        const matchingPercentage = calculateMatchingPercentage(data, fingerprints[key]);
        if (matchingPercentage > 0.7) {
            if (matchingPercentage > bestMatchPercentage) {
                bestMatchKey = key;
                bestMatchPercentage = matchingPercentage;
            }
        }
    }

    return bestMatchKey;
}

function extractDataFromFingerprints(data) {
    const result = {}

    Object.keys(fingerprints).forEach((key) => {
        result[key] = []
    })

    if (Array.isArray(data)) {
        data.forEach((item) => {
            const recurseResult = extractDataFromFingerprints(item);

            for (const key in recurseResult) {
                result[key].push(...recurseResult[key]);
            }

        })
    } else if (typeof data === 'object' && data !== null) {
        const bestMatchKey = findBestMatch(data);


        if (bestMatchKey) {
            // Add it to the result
            result[bestMatchKey].push(data);
        }

        // Recurse
        for (const key in data) {
            const recurseResult = extractDataFromFingerprints(data[key]);

            for (const recurseKey in recurseResult) {
                result[recurseKey].push(...recurseResult[recurseKey]);
            }
        }
    }


    return result
}


app.post('/api/discord', async (req, res) => {
    // console.log(JSON.stringify(req.body, null, 2));
    // Immediately send a response to the user
    res.send('Received!');

    const data = req.body.d;

    if (!data) return;

    // Walk 
    // Timestamp formatted  YYYY-MM-DDThh:mm:ssZ

    const timestamp = moment().format("YYYY-MM-DDThh:mm:ss")+'Z';

    const records = []

    for (key in extractDataFromFingerprints(data)) {
        for (const item of extractDataFromFingerprints(data)[key]) {
            // rename any "id" value to "discord_id"
            const newValue = _.cloneDeep(item);
            if (newValue.id) {
                newValue.discord_id = newValue.id;
                delete newValue.id;
            }

            const flattened = flat.flatten(newValue, {
                safe: true
            })

            // Delete any arrays of objects
            for (const flatKey in flattened) {
                if (isObjectArray(flattened[flatKey])) {
                    delete flattened[flatKey];
                }
            }

            // Delete any null values
            for (const flatKey in flattened) {
                if (flattened[flatKey] === null) {
                    delete flattened[flatKey];
                }
            }

            // Reformat any dates to match YYYY-MM-DDThh:mm:ssZ
            for (const flatKey in flattened) {
                if (flatKey.indexOf('timestamp') !== -1) {
                    flattened[flatKey] = moment(flattened[flatKey]).format("YYYY-MM-DDThh:mm:ss")+'Z';
                }

                // Check for date regex
                if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(flattened[flatKey])) {
                    flattened[flatKey] = moment(flattened[flatKey]).format("YYYY-MM-DDThh:mm:ss")+'Z';
                }
            }



            records.push({
                ...flattened,
                op_type: req.body.t,
                mina_type: key,
                id: uuid.v4(),
                timestamp: timestamp
            })
        }
    }

    // Convert to JSONL
    const jsonl = records.map((record) => {
        return JSON.stringify(record)
    }).join('\n')

    axios.post('http://10.0.5.1:8981/solr/MinaDiscord/update?commit=false', records)
    .then(({data}) => {
        console.log(data)
    })
    .catch((error) => {
        console.log(error)
    })

    // Close the connection
    res.end();
});

app.listen(3000, () => console.log('Server running on port 3000'));
