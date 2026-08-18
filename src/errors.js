export class QueueError extends Error {
  constructor(code, message, status = 400, details = undefined) {
    super(message);
    this.name = 'QueueError';
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

export const invalid = (message = 'request contract is invalid') => new QueueError('invalid_request', message, 400);
export const unauthorized = () => new QueueError('unauthorized', 'authentication is required', 401);
export const notFound = () => new QueueError('not_found', 'resource was not found', 404);
export const conflict = (message = 'stable identity is bound to a different intent') => new QueueError('conflict', message, 409);
export const fenced = (scope, current) => new QueueError('fenced', `${scope} is stale`, 409, { current });
export const outOfOrder = (scope, expected) => new QueueError(
  'out_of_order',
  `${scope} is not the next expected value`,
  409,
  { expected },
);
export const partitionBusy = () => new QueueError('partition_busy', 'the partition has an unfinished head intent', 503);
export const insufficientReplicas = (required, verified) => new QueueError(
  'insufficient_replicas',
  'the batch has not reached the configured replica gate',
  503,
  { required, verified },
);
export const offsetOutOfRange = (logStart, highWatermark) => new QueueError(
  'offset_out_of_range',
  'the requested offset is outside the retained committed range',
  409,
  { logStart, highWatermark },
);
export const corrupted = () => new QueueError('replica_corrupt_or_missing', 'no verified replica object is available', 503);
export const precondition = (current) => new QueueError('precondition_failed', 'the expected partition revision changed', 412, { current });
