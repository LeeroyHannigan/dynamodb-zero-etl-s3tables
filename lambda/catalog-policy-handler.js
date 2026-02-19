const { GlueClient, GetResourcePolicyCommand, PutResourcePolicyCommand } = require('@aws-sdk/client-glue');
const https = require('https');
const url = require('url');

const glue = new GlueClient();

async function sendResponse(event, status, physicalId, reason) {
  const body = JSON.stringify({
    Status: status,
    Reason: reason || '',
    PhysicalResourceId: physicalId,
    StackId: event.StackId,
    RequestId: event.RequestId,
    LogicalResourceId: event.LogicalResourceId,
  });

  const parsed = url.parse(event.ResponseURL);
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: parsed.hostname,
      path: parsed.path,
      method: 'PUT',
      headers: { 'Content-Type': '', 'Content-Length': body.length },
    }, resolve);
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

exports.handler = async (event) => {
  const { RequestType, ResourceProperties } = event;
  const { Statements, PolicyId } = ResourceProperties;
  const statements = JSON.parse(Statements);
  const ourSids = statements.map((s) => s.Sid);

  try {
    // Get existing policy
    let existingStatements = [];
    let policyHash;
    try {
      const existing = await glue.send(new GetResourcePolicyCommand({}));
      if (existing.PolicyInJson) {
        existingStatements = JSON.parse(existing.PolicyInJson).Statement || [];
        policyHash = existing.PolicyHash;
      }
    } catch (e) {
      if (e.name !== 'EntityNotFoundException') throw e;
    }

    // Remove our statements from existing policy
    const filtered = existingStatements.filter((s) => !ourSids.includes(s.Sid));

    if (RequestType === 'Create' || RequestType === 'Update') {
      const merged = [...filtered, ...statements];
      const params = {
        PolicyInJson: JSON.stringify({ Version: '2012-10-17', Statement: merged }),
        EnableHybrid: 'TRUE',
      };
      if (policyHash) params.PolicyHashCondition = policyHash;
      await glue.send(new PutResourcePolicyCommand(params));
    } else if (RequestType === 'Delete') {
      if (filtered.length > 0) {
        const params = {
          PolicyInJson: JSON.stringify({ Version: '2012-10-17', Statement: filtered }),
          EnableHybrid: 'TRUE',
        };
        if (policyHash) params.PolicyHashCondition = policyHash;
        await glue.send(new PutResourcePolicyCommand(params));
      }
    }

    await sendResponse(event, 'SUCCESS', PolicyId);
  } catch (e) {
    console.error(e);
    await sendResponse(event, 'FAILED', PolicyId, e.message);
  }
};
