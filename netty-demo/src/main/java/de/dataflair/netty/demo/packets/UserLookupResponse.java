package de.dataflair.netty.demo.packets;

import de.dataflair.netty.common.codec.CodecBuffer;
import de.dataflair.netty.common.packet.RespondPacket;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Response to {@link UserLookupRequest}. The query id is set by the transmitter, the responder only builds the
 * packet.
 */
public class UserLookupResponse extends RespondPacket {
    private boolean found;
    private String displayName = "";
    // Field initialisers run after super(buffer) but before readContent(...), which calls readBuffer(...).
    // That is why the payload is read there: values read in a base class constructor would be overwritten here.
    private List<String> roles = new ArrayList<>();

    public UserLookupResponse(boolean found, @NotNull String displayName, @NotNull List<String> roles) {
        this.found = found;
        this.displayName = displayName;
        this.roles = new ArrayList<>(roles);
    }

    public UserLookupResponse(@NotNull CodecBuffer buffer) {
        super(buffer);
    }

    public static UserLookupResponse notFound() {
        return new UserLookupResponse(false, "", List.of());
    }

    @Override
    public void writeBuffer(@NotNull CodecBuffer codecBuffer) {
        codecBuffer.writeBoolean(found)
                .writeString(displayName)
                .writeStringList(roles);
    }

    @Override
    public void readBuffer(@NotNull CodecBuffer codecBuffer) {
        this.found = codecBuffer.readBoolean();
        this.displayName = codecBuffer.readString();
        this.roles = codecBuffer.readStringList(new ArrayList<>());
    }

    public boolean found() {
        return found;
    }

    public String displayName() {
        return displayName;
    }

    public List<String> roles() {
        return roles;
    }

    @Override
    public String toString() {
        return found ? displayName + " " + roles : "<not found>";
    }
}
