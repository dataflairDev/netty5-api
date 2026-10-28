/*
 * Copyright 2023-2024 netty5-api contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import de.dataflair.netty5.actions.ConnectionAction;
import de.dataflair.netty5.server.Netty5Server;

public class DemoServer {
    public static void main(String[] args) {

        var server = new Netty5Server("127.0.0.1", 8080);

        try {
            server.initialize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        server.connectionFuture().thenAccept(unused -> {
            System.out.println("connected");
            server.packetTransmitter().listenQuery(DemoRequestPacket.class, demoRequestPacket -> new DemoRespondPacket(demoRequestPacket.s()));
        });

        server.addAction(new ConnectionAction(
                clientChannel -> clientChannel.transmitter().listenQuery(DemoRequestPacket.class, "asdasdasd", packet -> new DemoRespondPacket(packet.s())),
                ConnectionAction.State.CLIENT_AUTHENTICATED));
    }
}
