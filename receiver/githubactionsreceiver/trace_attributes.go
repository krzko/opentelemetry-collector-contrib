// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package githubactionsreceiver

import (
	"github.com/google/go-github/v61/github"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

func createResourceAttributes(resource pcommon.Resource, event interface{}, config *Config, logger *zap.Logger) {
	attrs := resource.Attributes()

	switch e := event.(type) {
	case *github.WorkflowJobEvent:
		serviceName := generateServiceName(config, e.Repo.GetFullName())
		attrs.PutStr("service.name", serviceName)

		attrs.PutStr("ci.github.workflow.name", e.WorkflowJob.GetWorkflowName())

		attrs.PutStr("ci.github.workflow.job.created_at", e.WorkflowJob.GetCreatedAt().Format(time.RFC3339))
		attrs.PutStr("ci.github.workflow.job.completed_at", e.WorkflowJob.GetCompletedAt().Format(time.RFC3339))
		attrs.PutStr("ci.github.workflow.job.conclusion", e.WorkflowJob.GetConclusion())
		attrs.PutStr("ci.github.workflow.job.head_branch", e.WorkflowJob.GetHeadBranch())
		attrs.PutStr("ci.github.workflow.job.head_sha", e.WorkflowJob.GetHeadSHA())
		attrs.PutStr("ci.github.workflow.job.html_url", e.WorkflowJob.GetHTMLURL())
		attrs.PutInt("ci.github.workflow.job.id", e.WorkflowJob.GetID())

		if len(e.WorkflowJob.Labels) > 0 {
			for i, label := range e.WorkflowJob.Labels {
				e.WorkflowJob.Labels[i] = strings.ToLower(label)
			}
			sort.Strings(e.WorkflowJob.Labels)
			joinedLabels := strings.Join(e.WorkflowJob.Labels, ",")
			attrs.PutStr("ci.github.workflow.job.labels", joinedLabels)
		} else {
			attrs.PutStr("ci.github.workflow.job.labels", "no labels")
		}

		attrs.PutStr("ci.github.workflow.job.name", e.WorkflowJob.GetName())
		attrs.PutInt("ci.github.workflow.job.run_attempt", e.WorkflowJob.GetRunAttempt())
		attrs.PutInt("ci.github.workflow.job.run_id", e.WorkflowJob.GetRunID())
		attrs.PutStr("ci.github.workflow.job.runner.group_name", e.WorkflowJob.GetRunnerGroupName())
		attrs.PutStr("ci.github.workflow.job.runner.name", e.WorkflowJob.GetRunnerName())
		attrs.PutStr("ci.github.workflow.job.sender.login", e.Sender.GetLogin())
		attrs.PutStr("ci.github.workflow.job.started_at", e.WorkflowJob.GetStartedAt().Format(time.RFC3339))
		attrs.PutStr("ci.github.workflow.job.status", e.WorkflowJob.GetStatus())

		attrs.PutStr("ci.system", "github")

		attrs.PutStr("scm.git.repo.owner.login", e.Repo.Owner.GetLogin())
		attrs.PutStr("scm.git.repo", e.Repo.GetFullName())

	case *github.WorkflowRunEvent:
		serviceName := generateServiceName(config, e.Repo.GetFullName())
		attrs.PutStr("service.name", serviceName)

		attrs.PutStr("ci.github.workflow.run.actor.login", e.WorkflowRun.GetActor().GetLogin())

		attrs.PutStr("ci.github.workflow.run.conclusion", e.WorkflowRun.GetConclusion())
		attrs.PutStr("ci.github.workflow.run.created_at", e.WorkflowRun.GetCreatedAt().Format(time.RFC3339))
		attrs.PutStr("ci.github.workflow.run.display_title", e.WorkflowRun.GetDisplayTitle())
		attrs.PutStr("ci.github.workflow.run.event", e.WorkflowRun.GetEvent())
		attrs.PutStr("ci.github.workflow.run.head_branch", e.WorkflowRun.GetHeadBranch())
		attrs.PutStr("ci.github.workflow.run.head_sha", e.WorkflowRun.GetHeadSHA())
		attrs.PutStr("ci.github.workflow.run.html_url", e.WorkflowRun.GetHTMLURL())
		attrs.PutInt("ci.github.workflow.run.id", e.WorkflowRun.GetID())
		attrs.PutStr("ci.github.workflow.run.name", e.WorkflowRun.GetName())
		attrs.PutStr("ci.github.workflow.run.path", e.Workflow.GetPath())

		if e.WorkflowRun.GetPreviousAttemptURL() != "" {
			htmlURL := transformGitHubAPIURL(e.WorkflowRun.GetPreviousAttemptURL())
			attrs.PutStr("ci.github.workflow.run.previous_attempt_url", htmlURL)
		}

		if len(e.WorkflowRun.ReferencedWorkflows) > 0 {
			var referencedWorkflows []string
			for _, workflow := range e.WorkflowRun.ReferencedWorkflows {
				referencedWorkflows = append(referencedWorkflows, workflow.GetPath())
			}
			attrs.PutStr("ci.github.workflow.run.referenced_workflows", strings.Join(referencedWorkflows, ";"))
		}

		attrs.PutInt("ci.github.workflow.run.run_attempt", int64(e.WorkflowRun.GetRunAttempt()))
		attrs.PutStr("ci.github.workflow.run.run_started_at", e.WorkflowRun.GetRunStartedAt().Format(time.RFC3339))
		attrs.PutStr("ci.github.workflow.run.status", e.WorkflowRun.GetStatus())
		attrs.PutStr("ci.github.workflow.run.sender.login", e.Sender.GetLogin())
		attrs.PutStr("ci.github.workflow.run.triggering_actor.login", e.WorkflowRun.GetTriggeringActor().GetLogin())
		attrs.PutStr("ci.github.workflow.run.updated_at", e.WorkflowRun.GetUpdatedAt().Format(time.RFC3339))

		attrs.PutStr("ci.system", "github")

		attrs.PutStr("scm.system", "git")

		attrs.PutStr("scm.git.head_branch", e.WorkflowRun.GetHeadBranch())
		attrs.PutStr("scm.git.head_commit.author.email", e.WorkflowRun.GetHeadCommit().GetAuthor().GetEmail())
		attrs.PutStr("scm.git.head_commit.author.name", e.WorkflowRun.GetHeadCommit().GetAuthor().GetName())
		attrs.PutStr("scm.git.head_commit.committer.email", e.WorkflowRun.GetHeadCommit().GetCommitter().GetEmail())
		attrs.PutStr("scm.git.head_commit.committer.name", e.WorkflowRun.GetHeadCommit().GetCommitter().GetName())
		attrs.PutStr("scm.git.head_commit.message", e.WorkflowRun.GetHeadCommit().GetMessage())
		attrs.PutStr("scm.git.head_commit.timestamp", e.WorkflowRun.GetHeadCommit().Timestamp.Format(time.RFC3339))
		attrs.PutStr("scm.git.head_sha", e.WorkflowRun.GetHeadSHA())

		if len(e.WorkflowRun.PullRequests) > 0 {
			var prUrls []string
			for _, pr := range e.WorkflowRun.PullRequests {
				prUrls = append(prUrls, convertPRURL(pr.GetURL()))
			}
			attrs.PutStr("scm.git.pull_requests.url", strings.Join(prUrls, ";"))
		}

		attrs.PutStr("scm.git.repo", e.Repo.GetFullName())

	default:
		logger.Error("unknown event type")
	}
}
