package util

object ListsConstants {

    // 🔎 Regras de sumarização (antigo Triple)
        // Spotify|Youtube|Deezer|Amazon|Pandora|SoundCloud|iTunes|TikTok|

    val PLAYERS = listOf(
        "Spotify",
        "Youtube",
        "Deezer",
        "Amazon",
        "Pandora",
        "SoundCloud",
        "iTunes",
        "TikTok"
    )


    /**
     * 1. ✔ Dados brutos: Gerado pelo próprio teste, usando os rawRows carregados do Redis
            * imusic:dashes:<plataforma>:<date>:rows
     * 2. ✔ Dados sumarizados: Lê do Redis e transforma para comparar
        * B: Resultados gerados pela pipeline
             * imusic:topplays:<plataforma>:<date>:rows
             * imusic:topplataform:<plataforma>:<date>:rows
             * imusic:topplaylist:<plataforma>:<date>:rows
             * imusic:topalbuns:<date>:rows
             * imusic:topalbum:<plataforma>:<date>:rows
             * imusic:topregiao:<date>:rows
             * imusic:topregioes:<plataforma>:<date>:rows
        3. ✔ Dumpes criados pelo teste
            * 📁 <summaryKey>_expected.json
            * 📁 <summaryKey>_from_redis.json
        Recalcula localmente:
         * ✔ Se o Redis não tiver a mesma chave, será null
         * ✔ Se tiver soma diferente, aparece divergência de valor
         * ✔ Verifica se quantidade bate
         * ✔ Verifica se todas keys esperadas existem no Redis
         * ✔ Verifica se o valor de number_of_streams é igual
         * ✔ chaves ausentes
         * ✔ keys agrupadas incorretamente
     */
    val SUMMARY_RULES = listOf(
        Triple("topplays",      "number_of_streams", "plays"),
        Triple("topplataform",  "plataform",         "plays"),
        Triple("topplaylist",   "plataform",         "plays"),
        Triple("topalbuns",     "upc",               "plays"),
        Triple("topalbum",      "upc",               "plays"),
        Triple("topplaysremunerado",     "territory",         "plays"),
        Triple("topregioes",    "territory",         "plays")
    )

    // 🟢 Etapas que liberam redis para continuação
    val FLOWS_STATUS = setOf(
        "process_all_files_complete",
        "sumarize_top_plays",
        "sumarize_top_plataform",
        "sumarize_top_playlist",
        "sumarize_top_albuns",
        "sumarize_top_plays_remunerado",
        "sumarize_top_regioes",
        "Finalizado",
        "completed"
    )

    val FLOWS_MESSAGE = setOf(
        "Validando configuração FUGA e S3",
        "Removendo arquivos antigos",
        "Limpando dados do Redis para o intervalo ...",
        "Buscando dados de analytics em FUGA Trends",
        "Executando a esteira de processamento dos arquivos (descompressao, upload para S3)",
        "Executando o envio dos dados para o REDIS",
        "Processamento concluído: X arquivos",
        "Sumarizando top plays",
        "Sumarizando top plataform",
        "Sumarizando top playlist",
        "Sumarizando top albuns",
        "Sumarizando top album",
        "Sumarizando top plays remunerado",
        "Sumarizando top regioes",
        "Iniciando processamento no backend externo",
        "Backend externo: ...",
        "Processamento externo concluído com sucesso",
        "Processamento completo (incluindo backend externo)"
    )

    // 📄 Colunas do TSV
    val TSV_COLUMNS = listOf(
        "label_id","product_id","asset_id","date","territory","number_of_downloads",
        "number_of_streams","number_of_listeners","number_of_saves","reporting_organization_id",
        "asset_duration","stream_device_type","stream_device_os","stream_length","stream_source",
        "stream_source_uri","user_id","user_region","user_region_detail","user_gender",
        "user_birth_year","user_age_group","user_country","user_access_type",
        "user_account_type","track_id","primary_artist_ids","isrc","upc","shuffle",
        "repeat","cached","completion","apple_container_type",
        "apple_container_sub_type","apple_source_of_stream","youtube_channel_id",
        "video_id","youtube_claimed_status","subscribed_status","asset_type",
        "discovery_flag","current_membership_week","first_trial_membership_week",
        "first_trial_membership","first_paid_membership_week","first_paid_membership",
        "total_user_streams","youtube_uploader_type","audio_format","dsp_data"
    )

    val SCHEMA_SUMARIZADO = mapOf(
        "topplaysremunerado" to listOf(
            "asset_id",
            "territory",
            "status",
            "number_of_streams",
            "date"
        ),
        "topplays" to listOf(
            "asset_id",
            "number_of_streams",
            "date",
            "status"
        ),
        "topalbum" to listOf(
            "upc",
            "plataform",
            "status",
            "number_of_streams",
            "date"
        ),
        "topalbuns" to listOf(
            "upc",
            "status",
            "number_of_streams",
            "date"
        ),
        "topplataform" to listOf(
            "asset_id",
            "plataform",
            "status",
            "number_of_streams",
            "date"
        ),
        "topplaylist" to listOf(
            "asset_id",
            "plataform",
            "stream_source",
            "stream_source_uri",
            "status",
            "number_of_streams",
            "date"
        ),
        "topregioes" to listOf(
            "asset_id",
            "territory",
            "plataform",
            "status",
            "number_of_streams",
            "date"
        )
    )


    val EXPECTED_PLAYERS = listOf(
        "iMusics_Amazon",
        "iMusics_Deezer",
        "iMusics_iTunes",
        "iMusics_TikTok",
        "iMusics_Pandora",
        "iMusics_Spotify",
        "iMusics_Youtube",
        "iMusics_SoundCloud"
    )
    val PLAYERS_ICON = mapOf(
        "iMusics_Amazon" to "🛒",
        "iMusics_Spotify" to "🎵",
        "iMusics_Deezer" to "📻",
        "iMusics_iTunes" to "🍎",
        "iMusics_TikTok" to "🎬",
        "iMusics_Pandora" to "📡",
        "iMusics_Youtube" to "▶️",
        "iMusics_SoundCloud" to "☁️"
    )

    // Campos esperados na CHAVE META
    val HASH_FIELDS_BY_ALL = listOf(
        "date",
        "file_name",
        "platform",
        "status",
        "timestamp",
        "total_items"
    )

    val HASH_FIELDS_BY_DASHES = listOf(
        "date",
        "file_name",
        "platform",
        "status",
        "timestamp",
        "reprocess",
        "row_count"
    )

    val HASH_FIELDS_BY_ALBUM_BY_PLAYREMUNERADO = listOf(
        "date",
        "file_name",
        "status",
        "timestamp",
        "total_items"
    )
}
