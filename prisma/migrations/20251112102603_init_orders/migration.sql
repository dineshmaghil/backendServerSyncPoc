-- CreateTable
CREATE TABLE `mh_off_orders` (
    `id` VARCHAR(36) NOT NULL,
    `location_id` VARCHAR(36) NOT NULL,
    `customer_id` VARCHAR(36) NULL,
    `order_no` VARCHAR(15) NOT NULL,
    `order_type_id` VARCHAR(36) NOT NULL,
    `order_date` DATE NOT NULL,
    `order_time` TIME(0) NOT NULL,
    `ip_address` VARCHAR(40) NOT NULL,
    `user_agent` VARCHAR(256) NOT NULL,
    `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
