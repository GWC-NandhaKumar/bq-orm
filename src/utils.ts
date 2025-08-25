import { DataType } from "./dataTypes";
import { Op, Operator } from "./op";

export function dataTypeToSchemaField(name: string, dt: DataType): any {
  let mode = dt.allowNull === false ? "REQUIRED" : "NULLABLE";
  if (dt.mode === "REPEATED") {
    mode = "REPEATED";
  }

  let type = dt.type;
  if (type === "STRUCT") {
    type = "STRUCT"; // or "RECORD"
    return {
      name,
      type,
      mode,
      fields: Object.entries(dt.fields || {}).map(([fieldName, fieldType]) =>
        dataTypeToSchemaField(fieldName, fieldType)
      ),
    };
  } else if (["NUMERIC", "BIGNUMERIC", "DECIMAL"].includes(type)) {
    return {
      name,
      type,
      mode,
      precision: dt.precision,
      scale: dt.scale,
    };
  } else {
    return { name, type, mode };
  }
}

export function buildWhereClause(
  where: any,
  params: Record<string, any> = {},
  paramIndex = 0
): { clause: string; params: Record<string, any>; nextIndex: number } {
  if (!where) return { clause: "", params: {}, nextIndex: paramIndex };

  const clauses: string[] = [];
  const localParams: Record<string, any> = {};

  for (const [key, value] of Object.entries(where)) {
    if (key === "and" || key === "or") {
      const subResults = (value as any[]).reduce(
        (acc, subCondition) => {
          const {
            clause,
            params: subParams,
            nextIndex,
          } = buildWhereClause(subCondition, acc.paramsAcc, acc.indexAcc);
          return {
            clauseAcc: [...acc.clauseAcc, `(${clause})`],
            paramsAcc: { ...acc.paramsAcc, ...subParams },
            indexAcc: nextIndex,
          };
        },
        {
          clauseAcc: [] as string[],
          paramsAcc: {} as Record<string, any>,
          indexAcc: paramIndex,
        }
      );
      clauses.push(subResults.clauseAcc.join(` ${Op[key as Operator]} `));
      Object.assign(localParams, subResults.paramsAcc);
      paramIndex = subResults.indexAcc;
    } else if (Array.isArray(value)) {
      const paramNames = value
        .map((v) => {
          const paramName = `param${paramIndex++}`;
          localParams[paramName] = v;
          return `@${paramName}`;
        })
        .join(", ");
      clauses.push(`\`${key}\` IN (${paramNames})`);
    } else if (
      typeof value === "object" &&
      value !== null &&
      !Array.isArray(value)
    ) {
      const opKey = Object.keys(value)[0] as Operator;
      const opVal = value[opKey as keyof typeof value];
      const sqlOp = Op[opKey] || "=";
      const paramName = `param${paramIndex++}`;
      clauses.push(`\`${key}\` ${sqlOp} @${paramName}`);
      localParams[paramName] = opVal;
    } else {
      const paramName = `param${paramIndex++}`;
      clauses.push(`\`${key}\` = @${paramName}`);
      localParams[paramName] = value;
    }
  }

  return {
    clause: clauses.join(" AND "),
    params: localParams,
    nextIndex: paramIndex,
  };
}
