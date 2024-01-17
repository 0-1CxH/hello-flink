package com.learnFlink.dataSourcesTestDrive;

import java.io.IOException;

public class debugPojoSchema {
    public static void main(String[] args) throws IOException {
        LogPojoSchema l = new LogPojoSchema();
        LogPOJO s = l.deserialize(
                "{\"datatime\":1645327532920,\"datatype\":\"process\",\"activity\":\"process_create\",\"platform\":\"linux\",\"event_id\":\"0x81010001\",\"unified_time\":1645327532920,\"severity\":0,\"layer\":\"host\",\"pid\":2833522,\"pguid\":\"660fa076-cb11-41e7-b855-c6b3f8b0cd3c\",\"pname\":\"sed\",\"path\":\"/usr/bin/sed\",\"uid\":0,\"uname\":\"root\",\"gid\":0,\"gname\":\"root\",\"md5\":\"5500939b02027edd627754ee92c83d3b\",\"cmd\":\"sed s/[[:space:]]//g \",\"ppid\":2833519,\"ppguid\":\"6aa720f9-cd6d-471d-9112-1aa5421ccf3a\",\"ppname\":\"sh\",\"pppath\":\"/usr/bin/bash\",\"ppuid\":-1,\"ppuname\":\"\",\"ppmd5\":\"4b9fb1497d240befc94ae633fcf3ecdb\",\"euid\":0,\"suid\":0,\"fsuid\":0,\"egid\":0,\"sgid\":0,\"fsgid\":0,\"euname\":\"root\",\"egname\":\"root\",\"auid\":0,\"session_guid\":\"701d46d0-a4e1-45ad-89c6-25b7d57c85e4\",\"tty\":\"(none)\",\"ftype\":\"regular file\",\"fctime\":1502955516,\"fsize\":76016,\"fmode\":\"100755\",\"fdev\":\"64769\",\"finode\":657552,\"funame\":\"root\",\"fgname\":\"root\",\"fatime\":1645326178,\"fmtime\":1402362110,\"create_type\":1,\"load_library\":\"\",\"agent_id\":\"6152873b378feae2\",\"event_guid\":\"63db64c0-3df2-4f5a-8bcd-94ca56fea3dc\",\"comid\":\"7989634363957ad93b03\",\"merge_count\":1,\"agent_ip\":\"111.172.239.146\",\"internal_ip\":[\"10.0.12.4\"],\"host_memo\":\"\",\"groupname\":\"自研连麦\",\"external_ip\":[\"111.172.239.146\"],\"host_name\":\"LINKMIC-StreamGate-Wuhan-CT-239-146.edgect.ali\",\"group\":2223}\n".getBytes()
        );
        System.out.println(s.toString());
    }

}
