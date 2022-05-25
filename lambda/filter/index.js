const filterKeys = (keys, limit) => {
    const keysToScan = keys.slice(0, limit);
    const remainingKeys = keys.slice(limit);
    return {
        keysToScan,
        remainingKeys
    };
};

exports.lambda_handler = async (event) => {
    const bucket = event.bucket;
    const allKeys = event.keys;
    const scanLimitPerIteration = event.limit;
    
    const filtered = filterKeys(allKeys, scanLimitPerIteration);
    
    const response = {
        keys: filtered.keysToScan,
        remainingKeys: filtered.remainingKeys? filtered.remainingKeys : null,
        bucket: bucket,
        limit: scanLimitPerIteration,
        remainingKeysLength: filtered.remainingKeys? filtered.remainingKeys.length : null
    };
    
    return response;
};
