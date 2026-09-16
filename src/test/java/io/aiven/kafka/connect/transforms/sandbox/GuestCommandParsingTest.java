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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins how {@code sandbox.command} is parsed, because the answer is surprising and silent.
 *
 * <p>Kafka parses a {@code ConfigDef.Type.LIST} by splitting on every comma, with no quoting and
 * no escape. An inline guest script -- the obvious thing to reach for when trying a runtime out --
 * is full of commas, so it does not arrive at the guest mangled or rejected: it arrives as a dozen
 * separate argv entries, and the process launches with nonsense. Nothing in Connect warns about
 * it.
 *
 * <p>These tests do not change that behaviour; changing it would mean leaving {@code ConfigDef}
 * behind. They exist so the limitation is written down somewhere that fails when it stops being
 * true, and so the documentation on {@code COMMAND_CONFIG} has something backing it.
 */
class GuestCommandParsingTest {

    private static SandboxTransformConfig configWithCommand(final String command) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(SandboxTransformConfig.COMMAND_CONFIG, command);
        return new SandboxTransformConfig(configs);
    }

    @Test
    void aCommandWithoutCommasSurvivesIntact() {
        assertThat(configWithCommand("python3,-u,/opt/guest.py").command())
            .containsExactly("python3", "-u", "/opt/guest.py");
    }

    @Test
    void anInlineScriptIsTornApartAtEveryComma() {
        // Exactly the shape of a hand-written guest: one -c argument holding real code.
        final String script = "import sys,json\nfor line in sys.stdin: sys.stdout.write(line)";

        final java.util.List<String> parsed =
            configWithCommand("python3,-u,-c," + script).command();

        // Four arguments went in. What comes out is not four arguments, and the script is no
        // longer one string -- it has been split straight through "sys,json".
        assertThat(parsed).hasSizeGreaterThan(4);
        assertThat(parsed).doesNotContain(script);
        assertThat(parsed).contains("import sys");

        // And the damage is silent: nothing rejects it, so the guest simply launches wrong.
        assertThat(parsed.get(0)).isEqualTo("python3");
    }

    @Test
    void anEmptyCommandIsTheOneCaseThatIsRejectedLoudly() {
        // SbxSandbox rejects an empty command, so at least that case is not a mystery at runtime.
        assertThat(configWithCommand("").command()).isEmpty();
    }
}
