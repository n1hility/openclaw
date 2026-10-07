// The Slack-only progress.reasoning setting in the merged config schema.
import { describe, expect, it } from "vitest";
import { buildConfigSchemaCore } from "./schema.js";

type SchemaNode = { properties?: Record<string, SchemaNode> };

function progressPropsFor(schema: SchemaNode, channelId: string) {
  return schema.properties?.channels?.properties?.[channelId]?.properties?.streaming?.properties
    ?.progress?.properties;
}

describe("config schema: Slack progress reasoning", () => {
  it("exposes channels.slack.streaming.progress.reasoning with its hint, on Slack only", () => {
    const res = buildConfigSchemaCore();
    const schema = res.schema as SchemaNode;

    expect(progressPropsFor(schema, "slack")).toHaveProperty("reasoning");
    expect(progressPropsFor(schema, "discord")).not.toHaveProperty("reasoning");
    expect(progressPropsFor(schema, "telegram")).not.toHaveProperty("reasoning");
    expect(res.uiHints["channels.slack.streaming.progress.reasoning"]?.label).toBe(
      "Slack Progress Reasoning Presentation",
    );
    expect(res.uiHints["channels.discord.streaming.progress.reasoning"]).toBeUndefined();
  });
});
