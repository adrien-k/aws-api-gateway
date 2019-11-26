const pRetry = require('p-retry')
const { utils } = require('@serverless/core')

const retry = (fn, opts = {}) => {
  return pRetry(
    async () => {
      try {
        return await fn()
      } catch (error) {
        if (error.code !== 'TooManyRequestsException') {
          // Stop retrying and throw the error
          throw new pRetry.AbortError(error)
        }
        throw error
      }
    },
    {
      retries: 5,
      minTimeout: 1000,
      factor: 2,
      ...opts
    }
  )
}

// Maximum 1 simultaneous calls and minimum 200ms between calls to AWS API
// This could be fine-tuned, but we quickly get TooManyRequests errors, especially
// when running this script from an AWS Lambda function
const MAX_CONCURRENCY = 1
const MIN_DELAY = 200
const requestQueue = []
let concurrency = 0
let timeout

const processNextQueueElement = () => {
  if (!requestQueue.length) {
    timeout = null
    return
  }

  if (concurrency < MAX_CONCURRENCY) {
    concurrency++
    const next = requestQueue.shift()
    next().then(() => {
      concurrency--
    })
  }

  timeout = setTimeout(processNextQueueElement, MIN_DELAY)
}

const throttleAwsRequestRequest = (awsRequest) => {
  return new Promise((resolve, reject) => {
    requestQueue.push(() => awsRequest.promise().then(resolve, reject))
    if (!timeout) {
      processNextQueueElement()
    }
  })
}

const apiExists = async ({ apig, apiId }) => {
  try {
    await throttleAwsRequestRequest(apig.getRestApi({ restApiId: apiId }))
    return true
  } catch (e) {
    if (e.code === 'NotFoundException') {
      return false
    }
    throw Error(e)
  }
}

const createApi = async ({ apig, name, description, endpointTypes }) => {
  const api = await throttleAwsRequestRequest(
    apig.createRestApi({
      name,
      description,
      endpointConfiguration: {
        types: endpointTypes
      }
    })
  )

  return api.id
}

const getPathId = async ({ apig, apiId, endpoint }) => {
  // todo this called many times to stay up to date. Is it worth the latency?
  const resourcesResponse = await throttleAwsRequestRequest(
    apig.getResources({
      restApiId: apiId
    })
  )
  const existingEndpoints = resourcesResponse.items

  if (!endpoint) {
    const rootResourceId = existingEndpoints.find(
      (existingEndpoint) => existingEndpoint.path === '/'
    ).id
    return rootResourceId
  }

  const endpointFound = existingEndpoints.find(
    (existingEndpoint) => existingEndpoint.path === endpoint.path
  )

  return endpointFound ? endpointFound.id : null
}

const endpointExists = async ({ apig, apiId, endpoint }) => {
  const resourceId = await retry(() => getPathId({ apig, apiId, endpoint }))

  if (!resourceId) {
    return false
  }

  const params = {
    httpMethod: endpoint.method,
    resourceId,
    restApiId: apiId
  }

  try {
    await retry(() => throttleAwsRequestRequest(apig.getMethod(params)))
    return true
  } catch (e) {
    if (e.code === 'NotFoundException') {
      return false
    }
  }
}

const myEndpoint = (state, endpoint) => {
  if (
    state.endpoints &&
    state.endpoints.find((e) => e.method === endpoint.method && e.path === endpoint.path)
  ) {
    return true
  }
  return false
}

const validateEndpointObject = ({ endpoint, apiId, stage, region }) => {
  const validMethods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS', 'ANY']

  if (typeof endpoint !== 'object') {
    throw Error('endpoint must be an object')
  }

  if (!endpoint.method) {
    throw Error(`missing method property for endpoint "${JSON.stringify(endpoint)}"`)
  }

  if (endpoint.path === '') {
    throw Error(
      `endpoint path cannot be an empty string for endpoint "${JSON.stringify(endpoint)}"`
    )
  }

  if (!endpoint.path) {
    throw Error(`missing path property for endpoint "${JSON.stringify(endpoint)}"`)
  }

  if (typeof endpoint.method !== 'string' || typeof endpoint.path !== 'string') {
    throw Error(`invalid endpoint "${JSON.stringify(endpoint)}"`)
  }

  if (!validMethods.includes(endpoint.method.toUpperCase())) {
    throw Error(`invalid method for endpoint "${JSON.stringify(endpoint)}"`)
  }

  if (endpoint.path !== '/') {
    if (!endpoint.path.startsWith('/')) {
      endpoint.path = `/${endpoint.path}`
    }
    if (endpoint.path.endsWith('/')) {
      endpoint.path = endpoint.path.substring(0, endpoint.path.length - 1)
    }
  }

  const validatedEndpoint = {
    url: `https://${apiId}.execute-api.${region}.amazonaws.com/${stage}${endpoint.path}`,
    path: endpoint.path,
    method: endpoint.method.toUpperCase()
  }

  return { ...endpoint, ...validatedEndpoint }
}

const validateEndpoint = async ({ apig, apiId, endpoint, state, stage, region }) => {
  const validatedEndpoint = validateEndpointObject({ endpoint, apiId, stage, region })

  if (await endpointExists({ apig, apiId, endpoint: validatedEndpoint })) {
    if (!myEndpoint(state, validatedEndpoint)) {
      throw Error(
        `endpoint ${validatedEndpoint.method} ${validatedEndpoint.path} already exists in provider`
      )
    }
  }

  return validatedEndpoint
}

const validateEndpoints = async ({ apig, apiId, endpoints, state, stage, region }) => {
  const promises = []

  for (const endpoint of endpoints) {
    promises.push(validateEndpoint({ apig, apiId, endpoint, state, stage, region }))
  }

  return Promise.all(promises)
}

const createPath = async ({ apig, apiId, endpoint }) => {
  const pathId = await getPathId({ apig, apiId, endpoint })

  if (pathId) {
    return pathId
  }

  const pathParts = endpoint.path.split('/')
  const pathPart = pathParts.pop()
  const parentEndpoint = { path: pathParts.join('/') }

  let parentId
  if (parentEndpoint.path === '') {
    parentId = await getPathId({ apig, apiId })
  } else {
    parentId = await createPath({ apig, apiId, endpoint: parentEndpoint })
  }

  const params = {
    pathPart,
    parentId,
    restApiId: apiId
  }

  const createdPath = await throttleAwsRequestRequest(apig.createResource(params))

  return createdPath.id
}

const createPaths = async ({ apig, apiId, endpoints }) => {
  const createdEndpoints = []

  for (const endpoint of endpoints) {
    endpoint.id = await createPath({ apig, apiId, endpoint })
    createdEndpoints.push(endpoint)
  }

  return createdEndpoints
}

const createMethod = async ({ apig, apiId, endpoint }) => {
  const params = {
    authorizationType: 'NONE',
    httpMethod: endpoint.method,
    resourceId: endpoint.id,
    restApiId: apiId,
    apiKeyRequired:  (typeof endpoint.apiKeyRequired !== "undefined") && endpoint.apiKeyRequired
  }

  if (endpoint.authorizerId) {
    params.authorizationType = 'CUSTOM'
    params.authorizerId = endpoint.authorizerId
  }

  if (endpoint.proxyURI && endpoint.greedyPath) {
    params.requestParameters = {
      [`method.request.path.${endpoint.greedyPath}`]: true
    }
  }

  try {
    await throttleAwsRequestRequest(apig.putMethod(params))
  } catch (e) {
    if (e.code === 'ConflictException' && endpoint.authorizerId) {
      // make sure authorizer config are always up to date
      const updateMethodParams = {
        httpMethod: endpoint.method,
        resourceId: endpoint.id,
        restApiId: apiId,
        patchOperations: [
          {
            op: 'replace',
            path: '/authorizationType',
            value: 'CUSTOM'
          },
          {
            op: 'replace',
            path: '/authorizerId',
            value: endpoint.authorizerId
          }
        ]
      }

      await throttleAwsRequestRequest(apig.updateMethod(updateMethodParams))
    } else if (e.code !== 'ConflictException') {
      throw Error(e)
    }
  }
}

const createMethods = async ({ apig, apiId, endpoints }) => {
  const promises = []

  for (const endpoint of endpoints) {
    promises.push(createMethod({ apig, apiId, endpoint }))
  }

  await Promise.all(promises)

  return endpoints
}

const createIntegration = async ({ apig, lambda, apiId, endpoint }) => {
  const isLambda = !!endpoint.function
  let functionName, accountId, region

  if (isLambda) {
    functionName = endpoint.function.split(':')[6]
    accountId = endpoint.function.split(':')[4]
    region = endpoint.function.split(':')[3] // todo what if the lambda in another region?
  }

  const integrationParams = {
    httpMethod: endpoint.method,
    resourceId: endpoint.id,
    restApiId: apiId,
    type: isLambda ? 'AWS_PROXY' : 'HTTP_PROXY',
    integrationHttpMethod: isLambda ? 'POST' : endpoint.method,
    uri: isLambda
      ? `arn:aws:apigateway:${region}:lambda:path/2015-03-31/functions/${endpoint.function}/invocations`
      : endpoint.proxyURI
  }

  if (endpoint.proxyURI && endpoint.greedyPath) {
    integrationParams.requestParameters = {
      [`integration.request.path.${endpoint.greedyPath}`]: `method.request.path.${endpoint.greedyPath}`
    }
  }

  try {
    await throttleAwsRequestRequest(apig.putIntegration(integrationParams))
  } catch (e) {
    if (e.code === 'ConflictException') {
      // this usually happens when there are too many endpoints for
      // the same function. Retrying after couple of seconds ensures
      // any pending integration requests are resolved.
      await utils.sleep(2000)
      return createIntegration({ apig, lambda, apiId, endpoint })
    }
    throw Error(e)
  }

  // Create lambda trigger for AWS_PROXY endpoints
  if (isLambda) {
    const permissionsParams = {
      Action: 'lambda:InvokeFunction',
      FunctionName: functionName,
      Principal: 'apigateway.amazonaws.com',
      SourceArn: `arn:aws:execute-api:${region}:${accountId}:${apiId}/*/*`,
      StatementId: `${functionName}-${apiId}`
    }

    try {
      await lambda.addPermission(permissionsParams).promise()
    } catch (e) {
      if (e.code !== 'ResourceConflictException') {
        throw Error(e)
      }
    }
  }

  return endpoint
}

const createIntegrations = async ({ apig, lambda, apiId, endpoints }) => {
  const promises = []

  for (const endpoint of endpoints) {
    promises.push(createIntegration({ apig, lambda, apiId, endpoint }))
  }

  return Promise.all(promises)
}

const createDeployment = async ({ apig, apiId, stage }) => {
  const deployment = await throttleAwsRequestRequest(
    apig.createDeployment({ restApiId: apiId, stageName: stage })
  )

  // todo add update stage functionality

  return deployment.id
}

const removeMethod = async ({ apig, apiId, endpoint }) => {
  const params = {
    restApiId: apiId,
    resourceId: endpoint.id,
    httpMethod: endpoint.method
  }

  try {
    await throttleAwsRequestRequest(apig.deleteMethod(params))
  } catch (e) {
    if (e.code !== 'NotFoundException') {
      throw Error(e)
    }
  }

  return {}
}

const removeMethods = async ({ apig, apiId, endpoints }) => {
  const promises = []

  for (const endpoint of endpoints) {
    promises.push(removeMethod({ apig, apiId, endpoint }))
  }

  return Promise.all(promises)
}

const removeResource = async ({ apig, apiId, endpoint }) => {
  try {
    await throttleAwsRequestRequest(
      apig.deleteResource({ restApiId: apiId, resourceId: endpoint.id })
    )
  } catch (e) {
    if (e.code !== 'NotFoundException') {
      throw Error(e)
    }
  }
  return {}
}

const removeResources = async ({ apig, apiId, endpoints }) => {
  const params = {
    restApiId: apiId
  }

  const resources = await throttleAwsRequestRequest(apig.getResources(params))

  const promises = []

  for (const endpoint of endpoints) {
    const resource = resources.items.find((resourceItem) => resourceItem.id === endpoint.id)

    const childResources = resources.items.filter(
      (resourceItem) => resourceItem.parentId === endpoint.id
    )

    const resourceMethods = resource ? Object.keys(resource.resourceMethods || {}) : []

    // only remove resources if they don't have methods nor child resources
    // to make sure we don't disrupt other services using the same api
    if (resource && resourceMethods.length === 0 && childResources.length === 0) {
      promises.push(removeResource({ apig, apiId, endpoint }))
    }
  }

  if (promises.length === 0) {
    return []
  }

  await Promise.all(promises)

  return removeResources({ apig, apiId, endpoints })
}

const removeApi = async ({ apig, apiId }) => {
  try {
    await throttleAwsRequestRequest(apig.deleteRestApi({ restApiId: apiId }))
  } catch (e) {}
}

const createAuthorizer = async ({ apig, lambda, apiId, endpoint }) => {
  if (endpoint.authorizer) {
    const authorizerName = endpoint.authorizer.split(':')[6]
    const region = endpoint.authorizer.split(':')[3]
    const accountId = endpoint.authorizer.split(':')[4]

    const authorizers = await throttleAwsRequestRequest(apig.getAuthorizers({ restApiId: apiId }))

    let authorizer = authorizers.items.find(
      (authorizerItem) => authorizerItem.name === authorizerName
    )

    if (!authorizer) {
      const createAuthorizerParams = {
        name: authorizerName,
        restApiId: apiId,
        type: 'TOKEN',
        authorizerUri: `arn:aws:apigateway:${region}:lambda:path/2015-03-31/functions/${endpoint.authorizer}/invocations`,
        identitySource: 'method.request.header.Auth'
      }

      authorizer = await throttleAwsRequestRequest(apig.createAuthorizer(createAuthorizerParams))

      const permissionsParams = {
        Action: 'lambda:InvokeFunction',
        FunctionName: authorizerName,
        Principal: 'apigateway.amazonaws.com',
        SourceArn: `arn:aws:execute-api:${region}:${accountId}:${apiId}/*/*`,
        StatementId: `${authorizerName}-${apiId}`
      }

      try {
        await lambda.addPermission(permissionsParams).promise()
      } catch (e) {
        if (e.code !== 'ResourceConflictException') {
          throw Error(e)
        }
      }
    }

    endpoint.authorizerId = authorizer.id
  }
  return endpoint
}

const createAuthorizers = async ({ apig, lambda, apiId, endpoints }) => {
  const updatedEndpoints = []

  for (const endpoint of endpoints) {
    endpoint.authorizerId = (await createAuthorizer({ apig, lambda, apiId, endpoint })).authorizerId
    updatedEndpoints.push(endpoint)
  }

  return updatedEndpoints
}

const removeAuthorizer = async ({ apig, apiId, endpoint }) => {
  // todo only remove authorizers that are not used by other services
  if (endpoint.authorizerId) {
    const updateMethodParams = {
      httpMethod: endpoint.method,
      resourceId: endpoint.id,
      restApiId: apiId,
      patchOperations: [
        {
          op: 'replace',
          path: '/authorizationType',
          value: 'NONE'
        }
      ]
    }

    await throttleAwsRequestRequest(apig.updateMethod(updateMethodParams))

    const deleteAuthorizerParams = { restApiId: apiId, authorizerId: endpoint.authorizerId }

    await throttleAwsRequestRequest(apig.deleteAuthorizer(deleteAuthorizerParams))
  }
  return endpoint
}

const removeAuthorizers = async ({ apig, apiId, endpoints }) => {
  const promises = []

  for (const endpoint of endpoints) {
    promises.push(removeAuthorizer({ apig, apiId, endpoint }))
  }

  await Promise.all(promises)

  return endpoints
}

const removeOutdatedEndpoints = async ({ apig, apiId, endpoints, stateEndpoints }) => {
  const outdatedEndpoints = []
  const outdatedAuthorizers = []
  for (const stateEndpoint of stateEndpoints) {
    const endpointInUse = endpoints.find(
      (endpoint) => endpoint.method === stateEndpoint.method && endpoint.path === stateEndpoint.path
    )

    const authorizerInUse = endpoints.find(
      (endpoint) => endpoint.authorizerId === stateEndpoint.authorizerId
    )

    if (!endpointInUse) {
      outdatedEndpoints.push(stateEndpoint)
    } else if (!authorizerInUse) {
      outdatedAuthorizers.push(stateEndpoint)
    }
  }

  await removeResources({ apig, apiId, endpoints: outdatedEndpoints })
  await removeMethods({ apig, apiId, endpoints: outdatedEndpoints })
  await removeAuthorizers({ apig, apiId, endpoints: outdatedAuthorizers })

  return outdatedEndpoints
}

module.exports = {
  validateEndpointObject,
  validateEndpoint,
  validateEndpoints,
  endpointExists,
  myEndpoint,
  apiExists,
  createApi,
  getPathId,
  createAuthorizer,
  createAuthorizers,
  createPath,
  createPaths,
  createMethod,
  createMethods,
  createIntegration,
  createIntegrations,
  createDeployment,
  removeMethod,
  removeMethods,
  removeResource,
  removeResources,
  removeAuthorizer,
  removeAuthorizers,
  removeApi,
  removeOutdatedEndpoints,
  retry
}
