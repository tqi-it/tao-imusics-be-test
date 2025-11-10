package util.configs

class Env {

    companion object {

        private var profile: String? = ""

        fun get(): String? {
            if(profile == "") {
                profile = System.getProperty("profile")
            }
            return profile
        }

        fun isProd() = get() == "prod"
        fun isQA() = get() == "qa"
        fun isDev() = get() == "dev"

    }

}