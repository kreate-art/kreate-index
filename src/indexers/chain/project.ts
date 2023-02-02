import { Address, ScriptHash } from "lucid-cardano";

import { deconstructAddress } from "@teiki/protocol/helpers/schema";
import * as S from "@teiki/protocol/schema";
import {
  ProjectDatum,
  ProjectDetailDatum,
  ProjectScriptDatum,
  ProjectStatus,
} from "@teiki/protocol/schema/teiki/project";
import { Cid, Hex, UnixTime } from "@teiki/protocol/types";
import { nullIfFalsy } from "@teiki/protocol/utils";

import { PROJECT_AT_TOKEN_NAMES } from "../../constants";
import { $handlers } from "../../framework/chain";
import { prettyOutRef } from "../../framework/chain/conversions";
import { Lovelace } from "../../types/chain";
import { NonEmpty } from "../../types/typelevel";

import { TeikiChainIndexContext } from "./context";

const ProjectStatusMapping = {
  Active: "active",
  PreClosed: "pre-closed",
  PreDelisted: "pre-delisted",
  Closed: "closed",
  Delisted: "delisted",
} as const satisfies { [K in ProjectStatus["type"]]: string };

export type ProjectStatusLiteral =
  (typeof ProjectStatusMapping)[keyof typeof ProjectStatusMapping];

export type ChainProject = {
  projectId: Hex;
  ownerAddress: Address;
  status: ProjectStatusLiteral;
  statusPendingUntil: UnixTime | null;
  milestoneReached: number;
  isStakingDelegationManagedByProtocol: boolean;
};

export type ChainProjectDetail = {
  projectId: Hex;
  withdrawnFunds: Lovelace;
  sponsoredUntil: UnixTime | null;
  informationCid: Cid;
  lastCommunityUpdateCid: Cid | null;
};

export type ChainProjectScript = {
  projectId: Hex;
  stakingKeyDeposit: Lovelace;
  stakingScriptHash: ScriptHash;
};

export type Event =
  | { type: "project"; indicies: NonEmpty<number[]> }
  | { type: "project_detail"; indicies: NonEmpty<number[]> }
  | { type: "project_script"; indicies: NonEmpty<number[]> }
  | { type: "project_script$ceased" };
const $ = $handlers<TeikiChainIndexContext, Event>();

export const setup = $.setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS chain.project (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL,
      owner_address text NOT NULL,
      status varchar(12) NOT NULL,
      status_pending_until timestamptz,
      milestone_reached smallint NOT NULL,
      is_staking_delegation_managed_by_protocol boolean NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_pid_index
      ON chain.project(project_id)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_status_index
      ON chain.project(status)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS chain.project_detail (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL,
      withdrawn_funds bigint NOT NULL,
      sponsored_until timestamptz,
      information_cid text NOT NULL,
      last_community_update_cid text
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_detail_pid_index
      ON chain.project_detail(project_id)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_detail_information_cid_index
      ON chain.project_detail(information_cid)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS chain.project_script (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL,
      staking_key_deposit bigint NOT NULL,
      staking_script_hash varchar(56) NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_script_pid_index
      ON chain.project_script(project_id)
  `;
});

export const filter = $.filter(
  ({
    tx,
    context: {
      config: { PROJECT_AT_MPH },
    },
  }) => {
    const projectIndicies: number[] = [];
    const projectDetailIndicies: number[] = [];
    const projectScriptIndicies: number[] = [];
    const MPH = `${PROJECT_AT_MPH}.`;
    for (const [index, output] of tx.body.outputs.entries()) {
      const assets = output.value.assets;
      if (assets == null) continue;
      // TODO: Integrity validation: An output should not contain
      // more than one type of the three tokens below
      if (assets[MPH + PROJECT_AT_TOKEN_NAMES.PROJECT] === 1n)
        projectIndicies.push(index);
      else if (assets[MPH + PROJECT_AT_TOKEN_NAMES.PROJECT_DETAIL] === 1n)
        projectDetailIndicies.push(index);
      else if (assets[MPH + PROJECT_AT_TOKEN_NAMES.PROJECT_SCRIPT] === 1n)
        projectScriptIndicies.push(index);
    }

    const events: Event[] = [];
    if (projectIndicies.length)
      events.push({ type: "project", indicies: projectIndicies });
    if (projectDetailIndicies.length)
      events.push({ type: "project_detail", indicies: projectDetailIndicies });
    if (projectScriptIndicies.length)
      events.push({ type: "project_script", indicies: projectScriptIndicies });
    if (
      (tx.body.mint.assets?.[MPH + PROJECT_AT_TOKEN_NAMES.PROJECT_SCRIPT] ??
        0) < 0
    )
      events.push({ type: "project_script$ceased" });
    return events;
  }
);

export const projectEvent = $.event<"project">(
  async ({ driver, connections: { sql, lucid }, event: { indicies } }) => {
    const projects = await driver.store(indicies, (output) => {
      if (output.datum == null) {
        console.warn(
          "datum should be available for project",
          prettyOutRef(output)
        );
        return undefined;
      }
      const projectDatum = S.fromData(S.fromCbor(output.datum), ProjectDatum);
      const projectId = projectDatum.projectId.id;
      const status = projectDatum.status;
      return [
        `project:${projectId}`,
        {
          projectId,
          status: ProjectStatusMapping[status.type],
          statusPendingUntil:
            "pendingUntil" in status
              ? Number(status.pendingUntil.timestamp)
              : null,
          ownerAddress: deconstructAddress(lucid, projectDatum.ownerAddress),
          milestoneReached: Number(projectDatum.milestoneReached),
          isStakingDelegationManagedByProtocol:
            projectDatum.isStakingDelegationManagedByProtocol,
        },
      ];
    });
    if (!projects.length) {
      console.warn("there is no valid project");
      return;
    }
    driver.refresh("views.project_summary");
    await sql`INSERT INTO chain.project ${sql(projects)}`;
  }
);

export const projectDetailEvent = $.event<"project_detail">(
  async ({ driver, connections: { sql }, event: { indicies } }) => {
    let hasCommunityUpdate = false;
    const projectDetails = await driver.store(indicies, (output) => {
      if (output.datum == null) {
        console.warn(
          "datum should be available for project detail",
          prettyOutRef(output)
        );
        return undefined;
      }
      const projectDetailDatum = S.fromData(
        S.fromCbor(output.datum),
        ProjectDetailDatum
      );
      const projectId = projectDetailDatum.projectId.id;
      if (projectDetailDatum.lastCommunityUpdateCid) hasCommunityUpdate = true;
      return [
        `project-detail:${projectId}`,
        {
          projectId,
          withdrawnFunds: projectDetailDatum.withdrawnFunds,
          sponsoredUntil: nullIfFalsy(
            Number(projectDetailDatum.sponsoredUntil?.timestamp)
          ),
          informationCid: projectDetailDatum.informationCid.cid,
          lastCommunityUpdateCid: nullIfFalsy(
            projectDetailDatum.lastCommunityUpdateCid?.cid
          ),
        },
      ];
    });
    if (!projectDetails.length) {
      console.warn("there is no valid project detail");
      return;
    }
    await sql`INSERT INTO chain.project_detail ${sql(projectDetails)}`;
    driver.notify("ipfs.project_content");
    hasCommunityUpdate && driver.notify("ipfs.project_community_update");
    driver.refresh("views.project_summary");
  }
);

export const projectScriptEvent = $.event<"project_script">(
  async ({
    driver,
    context: { staking },
    connections: { sql },
    event: { indicies },
  }) => {
    const projectScripts = await driver.storeWithScript(indicies, (output) => {
      if (output.scriptHash == null) {
        console.warn(
          "script reference should be available for project script",
          prettyOutRef(output)
        );
        return undefined;
      }
      if (output.datum == null) {
        console.warn(
          "datum should be available for project script",
          prettyOutRef(output)
        );
        return undefined;
      }
      const projectScriptDatum = S.fromData(
        S.fromCbor(output.datum),
        ProjectScriptDatum
      );
      const projectId = projectScriptDatum.projectId.id;
      return [
        `project-script:${projectId}`,
        {
          projectId,
          stakingKeyDeposit: projectScriptDatum.stakingKeyDeposit,
          stakingScriptHash: output.scriptHash,
        },
      ];
    });
    if (!projectScripts.length) {
      console.warn("there is no valid project script");
      return;
    }
    for (const script of projectScripts)
      staking.register(script.stakingScriptHash, "Script");
    await sql`INSERT INTO chain.project_script ${sql(projectScripts)}`;
  }
);
