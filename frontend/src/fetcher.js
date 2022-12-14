import config from './config.json'

const getSearchResults = async (query) => {
    console.log("Request is: ", `https://${config.server_host}:${config.server_port}/search?query=${query}`)
    const results = fetch(`https://${config.server_host}:${config.server_port}/search?query=${query}`, {
        method: 'GET',
    })
        .then(resp => resp.json())
        .then(respJson => {
            // console.log("From getSearchResults: ", respJson)
            return respJson["results"]
        })

    return results
}

const getImageResults = async (query) => {
    console.log("Request is: ", `https://${config.server_host}:${config.server_port}/image-search?query=${query}`)
    const results = fetch(`https://${config.server_host}:${config.server_port}/image-search?query=${query}`, {
        method: 'GET',
    })
        .then(resp => resp.json())
        .then(respJson => {
            // console.log("From getImageResults: ", respJson)
            return respJson["results"]
        })

    return results
}

const getShopping = async (query) => {
    console.log("Request is: ", `https://api.rainforestapi.com/request?api_key=12841E3B693C49A88A6ABC328237C093&type=search&amazon_domain=amazon.com&search_term=${query}`)
    const results = fetch(`https://api.rainforestapi.com/request?api_key=12841E3B693C49A88A6ABC328237C093&type=search&amazon_domain=amazon.com&search_term=${query}`, {
        method: 'GET',
    })
        .then(resp => resp.json())
        .then(respJson => {
            return respJson
        })

    return results
}

export {
    getSearchResults,
    getImageResults,
    getShopping,
} 