#!/usr/bin/env node
import { GitHubActionsClient } from './notifications-self-handoff.mjs';
import { requestRetirement } from './notifications-cloud-runtime.mjs';

const env = process.env;
const github = new GitHubActionsClient({ token: env.GH_TOKEN, repository: env.GITHUB_REPOSITORY });
const result = await requestRetirement({ github,
  targetCheckId: Number(env.RUNTIME_TARGET_CHECK_ID), expectedOwner: env.RUNTIME_TARGET_OWNER,
  expectedEpoch: Number(env.RUNTIME_TARGET_EPOCH), expectedRevision: env.RELEASE_SHA,
  expectedOrchestration: env.RUNTIME_TARGET_ORCHESTRATION_SHA
});
console.log(JSON.stringify({ event: 'controlled.retirement_acknowledged', ...result }));
