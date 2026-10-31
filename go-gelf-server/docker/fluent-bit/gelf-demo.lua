-- Acrescenta um campo `blob` de 12000 caracteres aleatorios ao registro, para
-- que o documento GELF, mesmo comprimido com gzip, passe do Packet_Size (1420)
-- e saia em varios chunks UDP. Aleatorio de proposito: um texto repetido seria
-- comprimido para menos de um chunk e nao provaria nada.
local alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
math.randomseed(os.time())

function add_blob(tag, timestamp, record)
    local parts = {}
    for i = 1, 12000 do
        local n = math.random(1, #alphabet)
        parts[i] = alphabet:sub(n, n)
    end
    record["blob"] = table.concat(parts)
    return 1, timestamp, record
end
