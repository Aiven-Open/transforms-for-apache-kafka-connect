/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.transforms.sandbox;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Signals that the isolation boundary failed, as opposed to the record being bad.
 *
 * <p>This deliberately extends {@link ConnectException} rather than
 * {@link org.apache.kafka.connect.errors.DataException}: a broken sandbox is a task-level fault,
 * whereas a record the guest rejected is a record-level fault that {@code errors.tolerance}
 * absorbs.
 *
 * <p>The distinction is real but Connect does not act on it as cleanly as the two types suggest.
 * Tolerance is decided by stage rather than by exception type, so {@code errors.tolerance=all}
 * skips the record for either. The type matters for what a reader of the logs and the DLQ sees,
 * and for the default {@code errors.tolerance=none}, where both fail the task. See
 * {@link SandboxTransform} for the full table, including the fact that the dead letter queue
 * exists for sink tasks only.
 */
public class SandboxException extends ConnectException {

    private static final long serialVersionUID = 1L;

    public SandboxException(final String message) {
        super(message);
    }

    public SandboxException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
